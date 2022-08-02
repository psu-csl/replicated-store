#include "multipaxos.h"
#include "json.h"

#include <chrono>
#include <thread>

using namespace std::chrono;

using nlohmann::json;

using grpc::ClientContext;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using multipaxos::Command;
using multipaxos::Instance;
using multipaxos::MultiPaxosRPC;

using multipaxos::InstanceState::INPROGRESS;

using multipaxos::ResponseType::OK;
using multipaxos::ResponseType::REJECT;

using multipaxos::AcceptRequest;
using multipaxos::AcceptResponse;
using multipaxos::HeartbeatRequest;
using multipaxos::HeartbeatResponse;
using multipaxos::PrepareRequest;
using multipaxos::PrepareResponse;

MultiPaxos::MultiPaxos(Log* log, json const& config)
    : ready_(false),
      ballot_(kMaxNumPeers),
      log_(log),
      id_(config["id"]),
      heartbeat_interval_(config["heartbeat_interval"]),
      engine_(id_),
      dist_(config["heartbeat_delta"],
            heartbeat_interval_ - static_cast<long>(config["heartbeat_delta"])),
      port_(config["peers"][id_]),
      last_heartbeat_(0),
      rpc_server_running_(false),
      thread_pool_(config["threadpool_size"]),
      heartbeat_thread_running_(false),
      prepare_thread_running_(false) {
  int64_t id = 0;
  for (std::string const peer : config["peers"])
    rpc_peers_.emplace_back(id++,
                            MultiPaxosRPC::NewStub(grpc::CreateChannel(
                                peer, grpc::InsecureChannelCredentials())));
}

void MultiPaxos::Start() {
  StartHeartbeatThread();
  StartPrepareThread();
  StartRPCServer();
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
  rpc_server_->Wait();
}

void MultiPaxos::StartHeartbeatThread() {
  DLOG(INFO) << id_ << " starting heartbeat thread";
  CHECK(!heartbeat_thread_running_);
  heartbeat_thread_running_ = true;
  heartbeat_thread_ = std::thread(&MultiPaxos::HeartbeatThread, this);
}

void MultiPaxos::StartPrepareThread() {
  DLOG(INFO) << id_ << " starting prepare thread";
  CHECK(!prepare_thread_running_);
  prepare_thread_running_ = true;
  prepare_thread_ = std::thread(&MultiPaxos::PrepareThread, this);
}

void MultiPaxos::Stop() {
  StopRPCServer();
  StopPrepareThread();
  StopHeartbeatThread();
  thread_pool_.join();
}

void MultiPaxos::StopRPCServer() {
  {
    std::unique_lock lock(mu_);
    while (!rpc_server_running_)
      rpc_server_running_cv_.wait(lock);
  }
  DLOG(INFO) << id_ << " stopping rpc server at " << port_;
  rpc_server_->Shutdown();
}

void MultiPaxos::StopHeartbeatThread() {
  DLOG(INFO) << id_ << " stopping heartbeat thread";
  CHECK(heartbeat_thread_running_);
  heartbeat_thread_running_ = false;
  cv_leader_.notify_one();
  heartbeat_thread_.join();
}

void MultiPaxos::StopPrepareThread() {
  DLOG(INFO) << id_ << " stopping prepare thread";
  CHECK(prepare_thread_running_);
  prepare_thread_running_ = false;
  cv_follower_.notify_one();
  prepare_thread_.join();
}

Result MultiPaxos::Replicate(Command command, client_id_t client_id) {
  auto [ballot, ready] = Ballot();
  if (IsLeader(ballot, id_)) {
    if (ready)
      return SendAccepts(ballot, log_->AdvanceLastIndex(), command, client_id);
    return Result{ResultType::kRetry, std::nullopt};
  }
  if (IsSomeoneElseLeader(ballot, id_))
    return Result{ResultType::kSomeoneElseLeader, Leader(ballot)};
  return Result{ResultType::kRetry, std::nullopt};
}

void MultiPaxos::HeartbeatThread() {
  while (heartbeat_thread_running_) {
    WaitUntilLeader();
    auto gle = log_->GlobalLastExecuted();
    while (heartbeat_thread_running_) {
      auto [ballot, ready] = Ballot();
      if (!IsLeader(ballot, id_))
        break;
      gle = SendHeartbeats(ballot, gle);
      SleepForHeartbeatInterval();
    }
  }
}

void MultiPaxos::PrepareThread() {
  while (prepare_thread_running_) {
    WaitUntilFollower();
    while (prepare_thread_running_) {
      SleepForRandomInterval();
      if (ReceivedHeartbeat())
        continue;
      auto ballot = NextBallot();
      Replay(ballot, SendPrepares(ballot));
      break;
    }
  }
}

int64_t MultiPaxos::SendHeartbeats(int64_t ballot,
                                   int64_t global_last_executed) {
  auto state = std::make_shared<heartbeat_state_t>(id_, log_->LastExecuted());

  HeartbeatRequest request;
  request.set_ballot(ballot);
  request.set_sender(id_);
  request.set_last_executed(state->min_last_executed_);
  request.set_global_last_executed(global_last_executed);

  for (auto& peer : rpc_peers_) {
    asio::post(thread_pool_, [this, state, &peer, request] {
      ClientContext context;
      HeartbeatResponse response;
      Status s = peer.stub_->Heartbeat(&context, std::move(request), &response);
      DLOG(INFO) << id_ << " sent heartbeat to " << peer.id_;
      {
        std::scoped_lock lock(state->mu_);
        ++state->num_rpcs_;
        if (s.ok()) {
          if (response.type() == OK) {
            ++state->num_oks_;
            if (response.last_executed() < state->min_last_executed_)
              state->min_last_executed_ = response.last_executed();
          } else {
            std::scoped_lock lock(mu_);
            if (response.ballot() >= ballot_) {
              SetBallot(response.ballot());
              state->leader_ = Leader(ballot_);
            }
          }
        }
      }
      state->cv_.notify_one();
    });
  }
  {
    std::unique_lock lock(state->mu_);
    while (state->leader_ == id_ && state->num_rpcs_ != rpc_peers_.size())
      state->cv_.wait(lock);
    if (state->num_oks_ == rpc_peers_.size())
      return state->min_last_executed_;
  }
  return global_last_executed;
}

std::optional<log_map_t> MultiPaxos::SendPrepares(int64_t ballot) {
  auto state = std::make_shared<prepare_state_t>(id_);

  PrepareRequest request;
  request.set_sender(id_);
  request.set_ballot(ballot);

  for (auto& peer : rpc_peers_) {
    asio::post(thread_pool_, [this, state, &peer, request] {
      ClientContext context;
      PrepareResponse response;
      Status s = peer.stub_->Prepare(&context, std::move(request), &response);
      DLOG(INFO) << id_ << " sent prepare request to " << peer.id_;
      {
        std::scoped_lock lock(state->mu_);
        ++state->num_rpcs_;
        if (s.ok()) {
          if (response.type() == OK) {
            ++state->num_oks_;
            for (int i = 0; i < response.instances_size(); ++i)
              Insert(&state->log_, std::move(response.instances(i)));
          } else {
            std::scoped_lock lock(mu_);
            if (response.ballot() >= ballot_) {
              SetBallot(response.ballot());
              state->leader_ = Leader(ballot_);
            }
          }
        }
      }
      state->cv_.notify_one();
    });
  }
  {
    std::unique_lock lock(state->mu_);
    while (state->leader_ == id_ && state->num_oks_ <= rpc_peers_.size() / 2 &&
           state->num_rpcs_ != rpc_peers_.size())
      state->cv_.wait(lock);
    if (state->num_oks_ > rpc_peers_.size() / 2)
      return std::move(state->log_);
  }
  return std::nullopt;
}

Result MultiPaxos::SendAccepts(int64_t ballot,
                               int64_t index,
                               Command command,
                               client_id_t client_id) {
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

  for (auto& peer : rpc_peers_) {
    asio::post(thread_pool_, [this, state, &peer, request] {
      ClientContext context;
      AcceptResponse response;
      Status s = peer.stub_->Accept(&context, std::move(request), &response);
      DLOG(INFO) << id_ << " sent accept request to " << peer.id_;
      {
        std::scoped_lock lock(state->mu_);
        ++state->num_rpcs_;
        if (s.ok()) {
          if (response.type() == OK) {
            ++state->num_oks_;
          } else {
            std::scoped_lock lock(mu_);
            if (response.ballot() >= ballot_) {
              SetBallot(response.ballot());
              state->leader_ = Leader(ballot_);
            }
          }
        }
      }
      state->cv_.notify_one();
    });
  }
  {
    std::unique_lock lock(state->mu_);
    while (state->leader_ == id_ && state->num_oks_ <= rpc_peers_.size() / 2 &&
           state->num_rpcs_ != rpc_peers_.size())
      state->cv_.wait(lock);
    if (state->num_oks_ > rpc_peers_.size() / 2)
      return Result{ResultType::kOk, std::nullopt};
    if (state->leader_ != id_)
      return Result{ResultType::kSomeoneElseLeader, state->leader_};
  }
  return Result{ResultType::kRetry, std::nullopt};
}

void MultiPaxos::Replay(int64_t ballot, std::optional<log_map_t> const& log) {
  if (!log)
    return;
  for (auto const& [index, instance] : *log) {
    Result r = SendAccepts(ballot, instance.index(), instance.command(),
                           instance.client_id());
    while (r.type_ == ResultType::kRetry)
      r = SendAccepts(ballot, instance.index(), instance.command(),
                      instance.client_id());
    if (r.type_ == ResultType::kSomeoneElseLeader)
      return;
  }
  ready_ = true;
  DLOG(INFO) << id_ << " leader is ready to serve";
}

Status MultiPaxos::Heartbeat(ServerContext*,
                             const HeartbeatRequest* request,
                             HeartbeatResponse* response) {
  DLOG(INFO) << id_ << " received heartbeat rpc from " << request->sender();
  std::scoped_lock lock(mu_);
  if (request->ballot() >= ballot_) {
    last_heartbeat_ = Now();
    SetBallot(request->ballot());
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

Status MultiPaxos::Prepare(ServerContext*,
                           const PrepareRequest* request,
                           PrepareResponse* response) {
  DLOG(INFO) << id_ << " received prepare rpc from " << request->sender();
  std::scoped_lock lock(mu_);
  if (request->ballot() >= ballot_) {
    SetBallot(request->ballot());
    for (auto& i : log_->InstancesSinceGlobalLastExecuted())
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
  DLOG(INFO) << id_ << " received accept rpc from " << request->sender();
  std::scoped_lock lock(mu_);
  if (request->instance().ballot() >= ballot_) {
    SetBallot(request->instance().ballot());
    log_->Append(request->instance());
    response->set_type(OK);
  } else {
    response->set_ballot(ballot_);
    response->set_type(REJECT);
  }
  return Status::OK;
}
