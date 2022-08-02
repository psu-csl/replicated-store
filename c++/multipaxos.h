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

inline int64_t Now() {
  return std::chrono::time_point_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now())
      .time_since_epoch()
      .count();
}

inline int64_t Leader(int64_t ballot) {
  return ballot & kIdBits;
}

inline bool IsLeader(int64_t ballot, int64_t id) {
  return Leader(ballot) == id;
}

inline bool IsSomeoneElseLeader(int64_t ballot, int64_t id) {
  return !IsLeader(ballot, id) && Leader(ballot) < kMaxNumPeers;
}

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
    auto old_leader = Leader(ballot_);
    auto new_leader = Leader(ballot);
    if (old_leader != new_leader &&
        (old_leader == id_ || old_leader == kMaxNumPeers)) {
      DLOG(INFO) << id_ << " became a follower: ballot: " << ballot_ << " -> "
                 << ballot;
      cv_follower_.notify_one();
    }
    ballot_ = ballot;
  }

  int64_t Id() const { return id_; }

  std::tuple<int64_t, bool> Ballot() const {
    std::scoped_lock lock(mu_);
    return {ballot_, is_ready_};
  }

  void WaitUntilLeader() {
    std::unique_lock lock(mu_);
    while (heartbeat_thread_running_ && !IsLeader(ballot_, id_))
      cv_leader_.wait(lock);
  }

  void WaitUntilFollower() {
    std::unique_lock lock(mu_);
    while (prepare_thread_running_ && IsLeader(ballot_, id_))
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
  void StartRPCServer();
  void StartHeartbeatThread();
  void StartPrepareThread();

  void Stop();
  void StopRPCServer();
  void StopHeartbeatThread();
  void StopPrepareThread();

  Result Replicate(multipaxos::Command command, client_id_t client_id);

  void HeartbeatThread();
  void PrepareThread();

  int64_t SendHeartbeats(int64_t ballot, int64_t global_last_executed);
  std::optional<log_map_t> SendPrepares(int64_t ballot);
  Result SendAccepts(int64_t ballot,
                     int64_t index,
                     multipaxos::Command command,
                     client_id_t client_id);

  void Replay(int64_t ballot, std::optional<log_map_t> const& log);

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

  std::atomic<bool> is_ready_;
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

  std::atomic<bool> heartbeat_thread_running_;
  std::thread heartbeat_thread_;

  std::atomic<bool> prepare_thread_running_;
  std::thread prepare_thread_;
};

struct heartbeat_state_t {
  heartbeat_state_t(int64_t leader, int64_t min_last_executed)
      : num_rpcs_(0),
        num_oks_(0),
        leader_(leader),
        min_last_executed_(min_last_executed) {}
  size_t num_rpcs_;
  size_t num_oks_;
  int64_t leader_;
  int64_t min_last_executed_ = 0;
  std::mutex mu_;
  std::condition_variable cv_;
};

struct prepare_state_t {
  explicit prepare_state_t(int64_t leader)
      : num_rpcs_(0), num_oks_(0), leader_(leader) {}
  size_t num_rpcs_;
  size_t num_oks_;
  int64_t leader_;
  log_map_t log_;
  std::mutex mu_;
  std::condition_variable cv_;
};

struct accept_state_t {
  explicit accept_state_t(int64_t leader)
      : num_rpcs_(0), num_oks_(0), leader_(leader) {}
  size_t num_rpcs_;
  size_t num_oks_;
  int64_t leader_;
  std::mutex mu_;
  std::condition_variable cv_;
};

#endif
