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

int64_t Now();

enum class ResultType {
  kOk,
  kRetry,
  kSomeoneElseLeader,
};

struct Result {
  ResultType type_;
  std::optional<int64_t> leader_;
};

class MultiPaxos : public multipaxos::MultiPaxosRPC::Service {
 public:
  MultiPaxos(Log* log, nlohmann::json const& config);
  MultiPaxos(MultiPaxos const& mp) = delete;
  MultiPaxos& operator=(MultiPaxos const& mp) = delete;
  MultiPaxos(MultiPaxos&& mp) = delete;
  MultiPaxos& operator=(MultiPaxos&& mp) = delete;

  int64_t NextBallot() {
    std::scoped_lock lock(mu_);
    auto old_ballot = ballot_;
    ballot_ += kRoundIncrement;
    ballot_ = (ballot_ & ~kIdBits) | id_;
    is_ready_ = false;
    DLOG(INFO) << id_ << " became a leader: ballot: " << old_ballot << " -> "
               << ballot_;
    cv_leader_.notify_one();
    return ballot_;
  }

  void SetBallot(int64_t ballot) {
    auto old_id = ballot_ & kIdBits;
    auto new_id = ballot & kIdBits;
    if ((old_id == id_ || old_id == kMaxNumPeers) && old_id != new_id) {
      DLOG(INFO) << id_ << " became a follower: ballot: " << ballot_ << " -> "
                 << ballot;
      cv_follower_.notify_one();
    }
    ballot_ = ballot;
  }

  std::tuple<bool, bool, int64_t> GetBallotOrLeader() const {
    std::scoped_lock lock(mu_);
    if (IsLeaderLockless())
      return {true, is_ready_, ballot_};
    return {false, false, LeaderLockless()};
  }

  int64_t Leader() const {
    std::scoped_lock lock(mu_);
    return LeaderLockless();
  }

  int64_t LeaderLockless() const { return ballot_ & kIdBits; }

  bool IsLeader() const {
    std::scoped_lock lock(mu_);
    return IsLeaderLockless();
  }

  bool IsLeaderLockless() const { return (ballot_ & kIdBits) == id_; }

  bool IsSomeoneElseLeader() const {
    std::scoped_lock lock(mu_);
    return IsSomeoneElseLeaderLockless();
  }

  bool IsSomeoneElseLeaderLockless() const {
    auto id = ballot_ & kIdBits;
    return id != id_ && id < kMaxNumPeers;
  }

  void WaitUntilLeader() {
    std::unique_lock lock(mu_);
    while (running_ && !IsLeaderLockless())
      cv_leader_.wait(lock);
  }

  void WaitUntilFollower() {
    std::unique_lock lock(mu_);
    while (running_ && IsLeaderLockless())
      cv_follower_.wait(lock);
  }

  void SleepForHeartbeatInterval() const {
    std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_interval_));
  }

  void SleepForRandomInterval() {
    auto sleep_time = heartbeat_interval_ + dist_(engine_);
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
  }

  bool ReceivedHeartbeat() const {
    return Now() - last_heartbeat_ < heartbeat_interval_;
  }

  void Start();
  void Stop();
  Result Replicate(multipaxos::Command command, client_id_t client_id);

  void HeartbeatThread();
  void PrepareThread();

  int64_t SendHeartbeats(int64_t ballot, int64_t global_last_executed);
  std::optional<log_map_t> SendPrepares(int64_t ballot);
  Result SendAccepts(int64_t ballot,
                     int64_t index,
                     multipaxos::Command command,
                     client_id_t client_id);

  bool Replay(log_map_t const& logs);

 private:
  grpc::Status Heartbeat(grpc::ServerContext*,
                         const multipaxos::HeartbeatRequest*,
                         multipaxos::HeartbeatResponse*) override;

  grpc::Status Prepare(grpc::ServerContext*,
                       const multipaxos::PrepareRequest*,
                       multipaxos::PrepareResponse*) override;

  grpc::Status Accept(grpc::ServerContext*,
                      const multipaxos::AcceptRequest*,
                      multipaxos::AcceptResponse*) override;

  std::atomic<bool> running_;
  bool is_ready_;
  int64_t ballot_;
  Log* log_;
  int64_t id_;
  long heartbeat_interval_;
  std::mt19937 engine_;
  std::uniform_int_distribution<int> dist_;
  std::string port_;
  std::atomic<long> last_heartbeat_;
  std::vector<rpc_peer_t> rpc_peers_;
  rpc_server_t rpc_server_;
  bool rpc_server_running_;
  std::condition_variable rpc_server_running_cv_;
  mutable std::mutex mu_;
  asio::thread_pool thread_pool_;

  std::condition_variable cv_leader_;
  std::condition_variable cv_follower_;

  std::thread heartbeat_thread_;
  std::thread prepare_thread_;
};

struct heartbeat_state_t {
  size_t num_rpcs_ = 0;
  size_t num_oks_ = 0;
  bool is_leader_ = true;
  int64_t min_last_executed = 0;
  std::mutex mu_;
  std::condition_variable cv_;
};

struct prepare_state_t {
  size_t num_rpcs_ = 0;
  size_t num_oks_ = 0;
  bool is_leader_ = true;
  log_map_t log_;
  std::mutex mu_;
  std::condition_variable cv_;
};

struct accept_state_t {
  size_t num_rpcs_ = 0;
  size_t num_oks_ = 0;
  bool is_leader_ = true;
  std::mutex mu_;
  std::condition_variable cv_;
};

#endif
