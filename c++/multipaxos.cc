#include "multipaxos.h"
#include "json.h"

#include <chrono>
#include <thread>

using namespace std::chrono;

using nlohmann::json;

using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using multipaxos::Command;
using multipaxos::Instance;
using multipaxos::MultiPaxosRPC;

using multipaxos::InstanceState::INPROGRESS;

using multipaxos::ResponseType::OK;
using multipaxos::ResponseType::REJECT;

using multipaxos::CommitRequest;
using multipaxos::CommitResponse;

using multipaxos::PrepareRequest;
using multipaxos::PrepareResponse;

using multipaxos::AcceptRequest;
using multipaxos::AcceptResponse;

MultiPaxos::MultiPaxos(Log* log, json const& config)
    : ballot_(kMaxNumPeers),
      log_(log),
      id_(config["id"]),
      commit_received_(false),
      commit_interval_(config["commit_interval"]),
      engine_(id_),
      port_(config["peers"][id_]),
      num_peers_(config["peers"].size()),
      thread_pool_(config["threadpool_size"]),
      rpc_server_running_(false),
      prepare_thread_running_(false),
      commit_thread_running_(false) {
  int64_t id = 0;
  for (std::string const peer : config["peers"])
    rpc_peers_.emplace_back(id++,
                            MultiPaxosRPC::NewStub(grpc::CreateChannel(
                                peer, grpc::InsecureChannelCredentials())));
}

void MultiPaxos::Start() {
  StartPrepareThread();
  StartCommitThread();
  StartRPCServer();
}

void MultiPaxos::Stop() {
  StopRPCServer();
  StopPrepareThread();
  StopCommitThread();
  thread_pool_.join();
}

void MultiPaxos::StartRPCServer() {
  DLOG(INFO) << id_ << " starting rpc server at " << port_;
  ServerBuilder builder;
  builder.AddListeningPort(port_, grpc::InsecureServerCredentials());
  builder.RegisterService(this);
  rpc_server_ = builder.BuildAndStart();
  {
    std::scoped_lock lock(mu_);
    rpc_server_running_ = true;
    rpc_server_running_cv_.notify_one();
  }
  rpc_server_thread_ = std::thread([this] { rpc_server_->Wait(); });
}

void MultiPaxos::StopRPCServer() {
  {
    std::unique_lock lock(mu_);
    while (!rpc_server_running_)
      rpc_server_running_cv_.wait(lock);
  }
  DLOG(INFO) << id_ << " stopping rpc server at " << port_;
  rpc_server_->Shutdown();
  rpc_server_thread_.join();
}

void MultiPaxos::StartPrepareThread() {
  DLOG(INFO) << id_ << " starting prepare thread";
  CHECK(!prepare_thread_running_);
  prepare_thread_running_ = true;
  prepare_thread_ = std::thread(&MultiPaxos::PrepareThread, this);
}

void MultiPaxos::StopPrepareThread() {
  DLOG(INFO) << id_ << " stopping prepare thread";
  CHECK(prepare_thread_running_);
  {
    std::unique_lock lock(mu_);
    prepare_thread_running_ = false;
  }
  cv_follower_.notify_one();
  prepare_thread_.join();
}

void MultiPaxos::StartCommitThread() {
  DLOG(INFO) << id_ << " starting commit thread";
  CHECK(!commit_thread_running_);
  commit_thread_running_ = true;
  commit_thread_ = std::thread(&MultiPaxos::CommitThread, this);
}

void MultiPaxos::StopCommitThread() {
  DLOG(INFO) << id_ << " stopping commit thread";
  CHECK(commit_thread_running_);
  {
    std::unique_lock lock(mu_);
    commit_thread_running_ = false;
  }
  cv_leader_.notify_one();
  commit_thread_.join();
}

Result MultiPaxos::Replicate(Command command, int64_t client_id) {
  auto ballot = Ballot();
  if (IsLeader(ballot, id_))
    return RunAcceptPhase(ballot, log_->AdvanceLastIndex(), std::move(command),
                          client_id);
  if (IsSomeoneElseLeader(ballot, id_))
    return Result{ResultType::kSomeoneElseLeader, ExtractLeaderId(ballot)};
  return Result{ResultType::kRetry, std::nullopt};
}

void MultiPaxos::PrepareThread() {
  while (prepare_thread_running_) {
    {
      std::unique_lock lock(mu_);
      while (prepare_thread_running_ && IsLeader(ballot_, id_))
        cv_follower_.wait(lock);
    }
    while (prepare_thread_running_) {
      SleepForRandomInterval();
      if (ReceivedCommit())
        continue;
      auto next_ballot = NextBallot();
      auto r = RunPreparePhase(next_ballot);
      if (r) {
        auto [max_last_index, log] = *r;
        BecomeLeader(next_ballot, max_last_index);
        Replay(next_ballot, log);
        break;
      }
    }
  }
}

void MultiPaxos::CommitThread() {
  while (commit_thread_running_) {
    {
      std::unique_lock lock(mu_);
      while (commit_thread_running_ && !IsLeader(ballot_, id_))
        cv_leader_.wait(lock);
    }
    auto gle = log_->GlobalLastExecuted();
    while (commit_thread_running_) {
      auto ballot = Ballot();
      if (!IsLeader(ballot, id_))
        break;
      gle = RunCommitPhase(ballot, gle);
      SleepForCommitInterval();
    }
  }
}

std::optional<
    std::pair<int64_t, std::unordered_map<int64_t, multipaxos::Instance>>>
MultiPaxos::RunPreparePhase(int64_t ballot) {
  auto state = std::make_shared<prepare_state_t>(id_);

  PrepareRequest request;
  request.set_sender(id_);
  request.set_ballot(ballot);

  std::vector<std::unique_ptr<ClientAsyncResponseReader<PrepareResponse>>> rpcs;
  std::vector<PrepareResponse> responses(num_peers_);
  std::vector<Status> statuses(num_peers_);
  std::vector<ClientContext> contexts(num_peers_);

  CompletionQueue cq;

  for (size_t i = 0; i < num_peers_; ++i) {
    rpcs.emplace_back(
        rpc_peers_[i].stub_->AsyncPrepare(&contexts[i], request, &cq));
    rpcs.back()->Finish(&responses[i], &statuses[i], (void*)i);
  }

  size_t num_rpcs = 0;
  size_t num_oks = 0;
  int64_t max_last_index = 0;
  std::unordered_map<int64_t, multipaxos::Instance> log;

  void* tag;
  bool ok = false;

  while (cq.Next(&tag, &ok)) {
    ++num_rpcs;
    if (ok) {
      auto response = &responses[reinterpret_cast<size_t>(tag)];
      if (response->type() == OK) {
        ++num_oks;
        for (int i = 0; i < response->instances_size(); ++i) {
          max_last_index =
              std::max(max_last_index, response->instances(i).index());
          Insert(&log, std::move(response->instances(i)));
        }
      } else {
        std::scoped_lock lock(mu_);
        if (response->ballot() > ballot_) {
          BecomeFollower(response->ballot());
          break;
        }
      }
    }
    if (num_oks > num_peers_ / 2)
      return std::make_pair(max_last_index, std::move(log));
  }
  return std::nullopt;
}

Result MultiPaxos::RunAcceptPhase(int64_t ballot,
                                  int64_t index,
                                  Command command,
                                  int64_t client_id) {
  auto state = std::make_shared<accept_state_t>(id_);

  Instance instance;
  instance.set_ballot(ballot);
  instance.set_index(index);
  instance.set_client_id(client_id);
  instance.set_state(INPROGRESS);
  *instance.mutable_command() = std::move(command);

  AcceptRequest request;
  request.set_sender(id_);
  *request.mutable_instance() = std::move(instance);

  std::vector<std::unique_ptr<ClientAsyncResponseReader<AcceptResponse>>> rpcs;
  std::vector<AcceptResponse> responses(num_peers_);
  std::vector<Status> statuses(num_peers_);
  std::vector<ClientContext> contexts(num_peers_);

  CompletionQueue cq;

  for (size_t i = 0; i < num_peers_; ++i) {
    rpcs.emplace_back(
        rpc_peers_[i].stub_->AsyncAccept(&contexts[i], request, &cq));
    rpcs.back()->Finish(&responses[i], &statuses[i], (void*)i);
  }

  size_t num_rpcs = 0;
  size_t num_oks = 0;

  void* tag;
  bool ok = false;

  while (cq.Next(&tag, &ok)) {
    ++num_rpcs;
    if (ok) {
      auto response = &responses[reinterpret_cast<size_t>(tag)];
      if (response->type() == OK) {
        ++num_oks;
      } else {
        std::scoped_lock lock(mu_);
        if (response->ballot() > ballot_) {
          BecomeFollower(response->ballot());
          return Result{ResultType::kSomeoneElseLeader, state->leader_};
        }
      }
    }
    if (num_oks > num_peers_ / 2) {
      log_->Commit(index);
      return Result{ResultType::kOk, std::nullopt};
    }
  }
  return Result{ResultType::kRetry, std::nullopt};
}

int64_t MultiPaxos::RunCommitPhase(int64_t ballot,
                                   int64_t global_last_executed) {
  auto state = std::make_shared<commit_state_t>(id_, log_->LastExecuted());

  CommitRequest request;
  request.set_ballot(ballot);
  request.set_sender(id_);
  request.set_last_executed(state->min_last_executed_);
  request.set_global_last_executed(global_last_executed);

  std::vector<std::unique_ptr<ClientAsyncResponseReader<CommitResponse>>> rpcs;
  std::vector<CommitResponse> responses(num_peers_);
  std::vector<Status> statuses(num_peers_);
  std::vector<ClientContext> contexts(num_peers_);

  CompletionQueue cq;

  for (size_t i = 0; i < num_peers_; ++i) {
    rpcs.emplace_back(
        rpc_peers_[i].stub_->AsyncCommit(&contexts[i], request, &cq));
    rpcs.back()->Finish(&responses[i], &statuses[i], (void*)i);
  }

  size_t num_rpcs = 0;
  size_t num_oks = 0;
  int64_t min_last_executed = log_->LastExecuted();

  void* tag;
  bool ok = false;

  while (cq.Next(&tag, &ok)) {
    ++num_rpcs;
    if (ok) {
      auto response = &responses[reinterpret_cast<size_t>(tag)];
      if (response->type() == OK) {
        ++num_oks;
        min_last_executed =
            std::min(min_last_executed, response->last_executed());
      } else {
        std::scoped_lock lock(mu_);
        if (response->ballot() > ballot_) {
          BecomeFollower(response->ballot());
          break;
        }
      }
    }
    if (num_oks == num_peers_)
      return min_last_executed;
  }
  return global_last_executed;
}

void MultiPaxos::Replay(
    int64_t ballot,
    std::unordered_map<int64_t, multipaxos::Instance> const& log) {
  for (auto const& [index, instance] : log) {
    Result r = RunAcceptPhase(ballot, instance.index(), instance.command(),
                              instance.client_id());
    while (r.type_ == ResultType::kRetry)
      r = RunAcceptPhase(ballot, instance.index(), instance.command(),
                         instance.client_id());
    if (r.type_ == ResultType::kSomeoneElseLeader)
      return;
  }
}

Status MultiPaxos::Prepare(ServerContext*,
                           const PrepareRequest* request,
                           PrepareResponse* response) {
  std::scoped_lock lock(mu_);
  DLOG(INFO) << id_ << " <--prepare-- " << request->sender();
  if (request->ballot() > ballot_) {
    BecomeFollower(request->ballot());
    for (auto& i : log_->Instances())
      *response->add_instances() = std::move(i);
    response->set_type(OK);
  } else {
    response->set_ballot(ballot_);
    response->set_type(REJECT);
  }
  return Status::OK;
}

Status MultiPaxos::Accept(ServerContext*,
                          const AcceptRequest* request,
                          AcceptResponse* response) {
  std::scoped_lock lock(mu_);
  DLOG(INFO) << id_ << " <--accept--- " << request->sender();
  if (request->instance().ballot() >= ballot_) {
    BecomeFollower(request->instance().ballot());
    log_->Append(request->instance());
    response->set_type(OK);
  } else {
    response->set_ballot(ballot_);
    response->set_type(REJECT);
  }
  return Status::OK;
}

Status MultiPaxos::Commit(ServerContext*,
                          const CommitRequest* request,
                          CommitResponse* response) {
  std::scoped_lock lock(mu_);
  DLOG(INFO) << id_ << " <--commit--- " << request->sender();
  if (request->ballot() >= ballot_) {
    commit_received_ = true;
    BecomeFollower(request->ballot());
    log_->CommitUntil(request->last_executed(), request->ballot());
    log_->TrimUntil(request->global_last_executed());
    response->set_last_executed(log_->LastExecuted());
    response->set_type(OK);
  } else {
    response->set_ballot(ballot_);
    response->set_type(REJECT);
  }
  return Status::OK;
}
