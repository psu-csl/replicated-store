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
#include <random>
#include <vector>

#include "json_fwd.h"
#include "kvstore.h"
#include "log.h"
#include "multipaxos.grpc.pb.h"

static const int64_t kIdBits = 0xff;
static const int64_t kRoundIncrement = kIdBits + 1;
static const int64_t kMaxNumPeers = 0xf;

class MultiPaxos : public multipaxos::MultiPaxosRPC::Service {
 public:
  MultiPaxos(Log* log, nlohmann::json const& config);
  MultiPaxos(Log const& log) = delete;
  MultiPaxos& operator=(MultiPaxos const& log) = delete;
  MultiPaxos(MultiPaxos&& log) = delete;
  MultiPaxos& operator=(MultiPaxos&& log) = delete;

  int64_t NextBallot() {
    std::scoped_lock lock(mu_);
    ballot_ += kRoundIncrement;
    ballot_ = (ballot_ & ~kIdBits) | id_;
    cv_leader_.notify_one();
    return ballot_;
  }

  int64_t Leader() const {
    std::scoped_lock lock(mu_);
    return ballot_ & kIdBits;
  }

  bool IsLeader() const {
    std::scoped_lock lock(mu_);
    return IsLeaderLockless();
  }

  bool IsLeaderLockless() const { return (ballot_ & kIdBits) == id_; }

  bool IsSomeoneElseLeader() const {
    std::scoped_lock lock(mu_);
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

  void Start();
  void Shutdown();

  void HeartbeatThread();
  void PrepareThread();

 private:
  grpc::Status Heartbeat(grpc::ServerContext*,
                         const multipaxos::HeartbeatRequest*,
                         multipaxos::HeartbeatResponse*) override;

  grpc::Status Prepare(grpc::ServerContext*,
                       const multipaxos::PrepareRequest*,
                       multipaxos::PrepareResponse*) override;

  std::atomic<bool> running_;
  int64_t ballot_;
  Log* log_;
  int64_t id_;
  long heartbeat_interval_;
  std::default_random_engine engine_;
  std::uniform_int_distribution<int> dist_;
  std::string port_;
  std::atomic<long> last_heartbeat_;
  std::vector<std::unique_ptr<multipaxos::MultiPaxosRPC::Stub>> rpc_peers_;
  std::unique_ptr<grpc::Server> rpc_server_;
  mutable std::mutex mu_;
  asio::thread_pool thread_pool_;

  std::thread heartbeat_thread_;
  std::condition_variable cv_leader_;
  multipaxos::HeartbeatRequest heartbeat_request_;
  size_t heartbeat_num_responses_;
  std::vector<int64_t> heartbeat_ok_responses_;
  std::mutex heartbeat_mu_;
  std::condition_variable heartbeat_cv_;

  std::thread prepare_thread_;
  std::condition_variable cv_follower_;
  multipaxos::PrepareRequest prepare_request_;
};

#endif
