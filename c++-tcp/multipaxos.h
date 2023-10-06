#ifndef MULTI_PAXOS_H_
#define MULTI_PAXOS_H_

#include <glog/logging.h>
#include <asio.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <thread>
#include <vector>

#include "json_fwd.h"
#include "kvstore.h"
#include "log.h"
#include "msg.h"
#include "tcp.h"

static const int64_t kIdBits = 0xff;
static const int64_t kRoundIncrement = kIdBits + 1;
static const int64_t kMaxNumPeers = 0xf;

using tcp_link_t = std::shared_ptr<TcpLink>;

struct rpc_peer_t {
  rpc_peer_t(int64_t id, tcp_link_t stub) : id_(id), stub_(std::move(stub)) {}
  int64_t id_;
  tcp_link_t stub_;
};

enum class ResultType {
  kOk,
  kRetry,
  kSomeoneElseLeader,
};

struct Result {
  ResultType type_;
  std::optional<int64_t> leader_;
};

inline int64_t ExtractLeaderId(int64_t ballot) {
  return ballot & kIdBits;
}

inline bool IsLeader(int64_t ballot, int64_t id) {
  return ExtractLeaderId(ballot) == id;
}

inline bool IsSomeoneElseLeader(int64_t ballot, int64_t id) {
  return !IsLeader(ballot, id) && ExtractLeaderId(ballot) < kMaxNumPeers;
}

class MultiPaxos {
 public:
  MultiPaxos(Log* log, 
             nlohmann::json const& config, 
             asio::io_context* io_context);
  MultiPaxos(MultiPaxos const& mp) = delete;
  MultiPaxos& operator=(MultiPaxos const& mp) = delete;
  MultiPaxos(MultiPaxos&& mp) = delete;
  MultiPaxos& operator=(MultiPaxos&& mp) = delete;

  int64_t Id() const { return id_; }

  int64_t Ballot() const { return ballot_; }

  int64_t NextBallot() {
    int64_t next_ballot = ballot_;
    next_ballot += kRoundIncrement;
    next_ballot = (next_ballot & ~kIdBits) | id_;
    return next_ballot;
  }

  void BecomeLeader(int64_t new_ballot, int64_t new_last_index) {
    std::scoped_lock lock(mu_);
    DLOG(INFO) << id_ << " became a leader: ballot: " << ballot_ << " -> "
               << new_ballot;
    log_->SetLastIndex(new_last_index);
    ballot_ = new_ballot;
    cv_leader_.notify_one();
  }

  void BecomeFollower(int64_t new_ballot) {
    std::scoped_lock lock(mu_);
    if (new_ballot <= ballot_)
      return;
    auto old_leader_id = ExtractLeaderId(ballot_);
    auto new_leader_id = ExtractLeaderId(new_ballot);
    if (new_leader_id != id_ &&
        (old_leader_id == id_ || old_leader_id == kMaxNumPeers)) {
      DLOG(INFO) << id_ << " became a follower: ballot: " << ballot_ << " -> "
                 << new_ballot;
      cv_follower_.notify_one();
    }
    ballot_ = new_ballot;
  }

  void SleepForCommitInterval() const {
    std::this_thread::sleep_for(std::chrono::milliseconds(commit_interval_));
  }

  void SleepForRandomInterval() {
    auto ci = commit_interval_;
    std::uniform_int_distribution<> dist(0, ci / 2);
    auto sleep_time = ci + ci / 2 + dist(engine_);
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
  }

  bool ReceivedCommit() { return commit_received_.exchange(false); }

  rpc_peer_t& Stub(int id) { return peers_[id]; }

  void Start();
  void Stop();

  void StartPrepareThread();
  void StopPrepareThread();

  void StartCommitThread();
  void StopCommitThread();

  void StopStub();

  Result Replicate(Command command, int64_t client_id);

  void PrepareThread();
  void CommitThread();

  std::optional<
      std::pair<int64_t, std::unordered_map<int64_t, Instance>>>
  RunPreparePhase(int64_t ballot);
  Result RunAcceptPhase(int64_t ballot,
                        int64_t index,
                        Command command,
                        int64_t client_id);
  int64_t RunCommitPhase(int64_t ballot, int64_t global_last_executed);

  void Replay(int64_t ballot,
              std::unordered_map<int64_t, Instance> const& log);

  BlockingReaderWriterQueue<std::string> AddChannel(int64_t channel_id);
  void RemoveChannel(int64_t channel_id);
  int64_t NextChannelId();

  PrepareResponse Prepare(json const& request);
  AcceptResponse Accept(json const& request);
  CommitResponse Commit(json const& request);

 private:
  std::atomic<int64_t> ballot_;
  Log* log_;
  int64_t id_;
  std::atomic<bool> commit_received_;
  long commit_interval_;
  std::mt19937 engine_;
  std::string port_;
  size_t num_peers_;
  std::vector<rpc_peer_t> peers_;
  std::atomic<int64_t> next_channel_id_;
  // ChannelMap channels_;
  mutable std::mutex mu_;
  asio::thread_pool thread_pool_;

  std::condition_variable cv_leader_;
  std::condition_variable cv_follower_;

  std::atomic<bool> prepare_thread_running_;
  std::thread prepare_thread_;

  std::atomic<bool> commit_thread_running_;
  std::thread commit_thread_;
};

#endif
