#include "multipaxos.h"
#include "json.h"

#include <chrono>
#include <thread>

using namespace std::chrono;

using moodycamel::BlockingReaderWriterQueue;
using nlohmann::json;

MultiPaxos::MultiPaxos(Log* log, 
                       json const& config, 
                       asio::io_context* io_context)
    : ballot_(kMaxNumPeers),
      log_(log),
      id_(config["id"]),
      commit_received_(false),
      commit_interval_(config["commit_interval"]),
      engine_(id_),
      port_(config["peers"][id_]),
      num_peers_(config["peers"].size()),
      next_channel_id_(0),
      thread_pool_(config["threadpool_size"]),
      prepare_thread_running_(false),
      commit_thread_running_(false) {
  int64_t id = 0;
  for (std::string const peer : config["peers"]) {
      peers_.emplace_back(id++, 
          std::make_shared<TcpLink>(peer, io_context));
  }
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
    std::pair<int64_t, std::unordered_map<int64_t, Instance>>>
MultiPaxos::RunPreparePhase(int64_t ballot) {
  size_t num_oks = 0;
  std::unordered_map<int64_t, Instance> log;
  int64_t max_last_index = 0;

  if (ballot > ballot_) {
    ++num_oks;
    log = log_->GetLog();
    max_last_index = log_->LastIndex();
    if (num_oks > num_peers_ / 2) {
      return std::make_pair(max_last_index, std::move(log));
    }
  } else {
    return std::nullopt;
  }

  PrepareRequest prepare_request(ballot, id_);
  json request = prepare_request;
  int64_t channel_id = NextChannelId();
  BlockingConcurrentQueue<std::string> channel(num_peers_ - 1);

  for (auto& peer : peers_) {
    if (peer.id_ != id_) {
      asio::post(thread_pool_, [this, &peer, request, channel_id, &channel] {
        peer.stub_->SendAwaitResponse(PREPAREREQUEST, channel_id, channel,
                                      request.dump());
        DLOG(INFO) << id_ << " sent prepare request to " << peer.id_;
      });
    }
  }

  while (true) {
    std::string response;
    channel.wait_dequeue_timed(response, std::chrono::milliseconds(500));
    if (response.size() == 0) {
      break;
    }
    json prepare_response = json::parse(response);
    if (prepare_response["type_"] == OK) {
      ++num_oks;
      auto instances = prepare_response["instances_"]
          .get<std::vector<Instance>>();
      for (auto instance : instances) {
        max_last_index = std::max(max_last_index, instance.index_);
        Insert(&log, std::move(instance));
      }
    } else {
      BecomeFollower(prepare_response["ballot_"]);
      break;
    }
    if (num_oks > num_peers_ / 2) {
      // RemoveChannel(channel_id);
      return std::make_pair(max_last_index, std::move(log));
    }
  }
  // RemoveChannel(channel_id);
  return std::nullopt;
}

Result MultiPaxos::RunAcceptPhase(int64_t ballot,
                                  int64_t index,
                                  Command command,
                                  int64_t client_id) {
  size_t num_oks = 0;

  Instance instance(ballot, index, client_id, INPROGRESS, command);

  if (ballot == ballot_) {
    ++num_oks;
    log_->Append(instance);
    if (num_oks > num_peers_ / 2) {
      log_->Commit(index);
      return Result{ResultType::kOk, std::nullopt};
    }
  } else {
    auto leader = ExtractLeaderId(ballot_);
    return Result{ResultType::kSomeoneElseLeader, leader};
  }

  AcceptRequest accept_request(instance, id_);
  json request = accept_request;
  int64_t channel_id = NextChannelId();
  BlockingConcurrentQueue<std::string> channel(num_peers_ - 1);

  for (auto& peer : peers_) {
    if (peer.id_ != id_) {
      asio::post(thread_pool_, [this, &peer, request, channel_id, &channel] {
        peer.stub_->SendAwaitResponse(ACCEPTREQUEST, channel_id, channel,
                                      request.dump());
        DLOG(INFO) << id_ << " sent accept request to " << peer.id_;
      });
    }
  }

  while (true) {
    std::string response;
    channel.wait_dequeue_timed(response, std::chrono::milliseconds(3000));
    if (response.size() == 0) {
      break;
    }
    json accept_response = json::parse(response);
    if (accept_response["type_"] == OK)
      ++num_oks;
    else {
      BecomeFollower(accept_response["ballot_"]);
      break;
    }
    if (num_oks > num_peers_ / 2) {
      log_->Commit(index);
      // RemoveChannel(channel_id);
      return Result{ResultType::kOk, std::nullopt};
    }
  }
  // RemoveChannel(channel_id);
  if (!IsLeader(ballot_, id_))
    return Result{ResultType::kSomeoneElseLeader, ExtractLeaderId(ballot_)};
  return Result{ResultType::kRetry, std::nullopt};
}

int64_t MultiPaxos::RunCommitPhase(int64_t ballot,
                                   int64_t global_last_executed) {
  size_t num_oks = 0;
  int64_t min_last_executed = log_->LastExecuted();

  ++num_oks;
  log_->TrimUntil(global_last_executed);
  if (num_oks == num_peers_)
    return min_last_executed;

  CommitRequest commit_request(ballot, min_last_executed, global_last_executed, id_);
  json request = commit_request;
  int64_t channel_id = NextChannelId();
  BlockingConcurrentQueue<std::string> channel(num_peers_ - 1);

  for (auto& peer : peers_) {
    if (peer.id_ != id_) {
      asio::post(thread_pool_, [this, &peer, request, channel_id, &channel] {
        peer.stub_->SendAwaitResponse(COMMITREQUEST, channel_id, channel,
                                      request.dump());
        DLOG(INFO) << id_ << " sent commit to " << peer.id_;
      });
    }
  }

  while (true) {
    std::string response;
    channel.wait_dequeue_timed(response, std::chrono::milliseconds(commit_interval_));
    if (response.size() == 0) {
      break;
    }
    json commit_response = json::parse(response);
    if (commit_response["type_"] == OK) {
      ++num_oks;
      if (commit_response["last_executed_"] < min_last_executed)
        min_last_executed = commit_response["last_executed_"];
    } else {
      BecomeFollower(commit_response["ballot_"]);
      break;
    }
    if (num_oks == num_peers_) {
      // RemoveChannel(channel_id);
      return min_last_executed;
    }
  }
  // RemoveChannel(channel_id);
  return global_last_executed;
}

void MultiPaxos::Replay(
    int64_t ballot,
    std::unordered_map<int64_t, Instance> const& log) {
  for (auto const& [index, instance] : log) {
    Result r = RunAcceptPhase(ballot, instance.index_, instance.command_,
                              instance.client_id_);
    while (r.type_ == ResultType::kRetry)
      r = RunAcceptPhase(ballot, instance.index_, instance.command_,
                         instance.client_id_);
    if (r.type_ == ResultType::kSomeoneElseLeader)
      return;
  }
}

// BlockingReaderWriterQueue<std::string> 
//     MultiPaxos::AddChannel(int64_t channel_id) {
//   BlockingReaderWriterQueue<std::string> response_chan(num_peers_ - 1);
//   std::unique_lock lock(channels_.mu_);
//   auto [it, ok] = channels_.map_.insert({channel_id, response_chan});
//   CHECK(ok);
//   return response_chan;
// }

// void MultiPaxos::RemoveChannel(int64_t channel_id) {
//   std::unique_lock lock(channels_.mu_);
//   auto it = channels_.map_.find(channel_id);
//   CHECK(it != channels_.map_.end());
//   channels_.map_.erase(it);
// }

int64_t MultiPaxos::NextChannelId() {
  return ++next_channel_id_;
}

void MultiPaxos::Start() {
  StartPrepareThread();
  StartCommitThread();
}

void MultiPaxos::Stop() {
  StopPrepareThread();
  StopCommitThread();
  StopStub();
  thread_pool_.join();
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

void MultiPaxos::StopStub() {
  for (auto& peer : peers_)
    peer.stub_->Stop();
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

PrepareResponse MultiPaxos::Prepare(json const& request) {
  DLOG(INFO) << id_ << " <--prepare-- " << request["sender_"];
  int64_t ballot = request.at("ballot_").get<int64_t>();
  if (ballot > ballot_) {
    BecomeFollower(request["ballot_"]);
    PrepareResponse response(OK, ballot_, log_->Instances());
    return response;
  } else {
    PrepareResponse response(REJECT, ballot_, std::vector<Instance>());
    return response;
  }
}

AcceptResponse MultiPaxos::Accept(json const& request) {
  DLOG(INFO) << id_ << " <--accept--- " << request["sender_"];
  int64_t ballot = request.at("instance_").at("ballot_").get<int64_t>();
  if (ballot >= ballot_) {
    auto instance = request.at("instance_").get<Instance>();
    log_->Append(instance);
    if (ballot > ballot_)
      BecomeFollower(request["instance_"]["ballot_"]);
    AcceptResponse response(OK, ballot_);
    return response;
  }
  if (ballot < ballot_) {
    AcceptResponse response(REJECT, ballot_);
    return response;
  }
  AcceptResponse response(OK, ballot_);
  return response;
}

CommitResponse MultiPaxos::Commit(json const& request) {
  DLOG(INFO) << id_ << " <--commit--- " << request.at("sender_");
  int64_t ballot = request.at("ballot_").get<int64_t>();
  if (ballot >= ballot_) {
    commit_received_ = true;
    log_->CommitUntil(request["last_executed_"], request["ballot_"]);
    log_->TrimUntil(request["global_last_executed_"]);
    if (ballot > ballot_)
      BecomeFollower(request["ballot_"]);
    CommitResponse response(OK, ballot_, log_->LastExecuted());
    return response;
  } else {
    CommitResponse response(REJECT, ballot_, 0);
    return response;
  }
}
