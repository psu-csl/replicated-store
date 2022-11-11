#ifndef MULTI_PAXOS_H_
#define MULTI_PAXOS_H_

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
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
#include "multipaxos.grpc.pb.h"

static const int64_t kIdBits = 0xff;
static const int64_t kRoundIncrement = kIdBits + 1;
static const int64_t kMaxNumPeers = 0xf;

using rpc_stub_t = std::unique_ptr<multipaxos::MultiPaxosRPC::Stub>;

struct rpc_peer_t {
  rpc_peer_t(int64_t id, rpc_stub_t stub) : id_(id), stub_(std::move(stub)) {}
  int64_t id_;
  rpc_stub_t stub_;
};

using rpc_server_t = std::unique_ptr<grpc::Server>;

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

class MultiPaxos : public multipaxos::MultiPaxosRPC::Service {
 public:
  MultiPaxos(Log* log, nlohmann::json const& config);
  MultiPaxos(MultiPaxos const& mp) = delete;
  MultiPaxos& operator=(MultiPaxos const& mp) = delete;
  MultiPaxos(MultiPaxos&& mp) = delete;
  MultiPaxos& operator=(MultiPaxos&& mp) = delete;

  int64_t Id() const { return id_; }

  int64_t Ballot() const {
    std::scoped_lock lock(mu_);
    return ballot_;
  }

  int64_t NextBallot() {
    std::scoped_lock lock(mu_);
    int64_t next_ballot = ballot_;
    next_ballot += kRoundIncrement;
    next_ballot = (next_ballot & ~kIdBits) | id_;
    return next_ballot;
  }

  void BecomeLeader(int64_t new_ballot, int64_t new_last_index) {
    std::scoped_lock lock(mu_);
    DLOG(INFO) << id_ << " became a leader: ballot: " << ballot_ << " -> "
               << new_ballot;
    ballot_ = new_ballot;
    log_->SetLastIndex(new_last_index);
    cv_leader_.notify_one();
  }

  void BecomeFollower(int64_t new_ballot) {
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

  void Start();
  void Stop();

  void StartRPCServer();
  void StopRPCServer();

  void StartPrepareThread();
  void StopPrepareThread();

  void StartCommitThread();
  void StopCommitThread();

  Result Replicate(multipaxos::Command command, int64_t client_id);

  void PrepareThread();
  void CommitThread();

  std::optional<
      std::pair<int64_t, std::unordered_map<int64_t, multipaxos::Instance>>>
  RunPreparePhase(int64_t ballot);
  Result RunAcceptPhase(int64_t ballot,
                        int64_t index,
                        multipaxos::Command command,
                        int64_t client_id);
  int64_t RunCommitPhase(int64_t ballot, int64_t global_last_executed);

  void Replay(int64_t ballot,
              std::unordered_map<int64_t, multipaxos::Instance> const& log);

 private:
  grpc::Status Prepare(grpc::ServerContext*,
                       const multipaxos::PrepareRequest*,
                       multipaxos::PrepareResponse*) override;

  grpc::Status Accept(grpc::ServerContext*,
                      const multipaxos::AcceptRequest*,
                      multipaxos::AcceptResponse*) override;

  grpc::Status Commit(grpc::ServerContext*,
                      const multipaxos::CommitRequest*,
                      multipaxos::CommitResponse*) override;

  int64_t ballot_;
  Log* log_;
  int64_t id_;
  std::atomic<bool> commit_received_;
  long commit_interval_;
  std::mt19937 engine_;
  std::string port_;
  size_t num_peers_;
  std::vector<rpc_peer_t> rpc_peers_;
  mutable std::mutex mu_;
  asio::thread_pool thread_pool_;

  std::condition_variable cv_leader_;
  std::condition_variable cv_follower_;

  rpc_server_t rpc_server_;
  bool rpc_server_running_;
  std::condition_variable rpc_server_running_cv_;
  std::thread rpc_server_thread_;

  std::atomic<bool> prepare_thread_running_;
  std::thread prepare_thread_;

  std::atomic<bool> commit_thread_running_;
  std::thread commit_thread_;
};

#endif
