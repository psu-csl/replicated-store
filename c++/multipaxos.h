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

using rpc_stub_t = std::unique_ptr<multipaxos::MultiPaxosRPC::Stub>;

struct rpc_peer_t {
  rpc_peer_t(int64_t id, rpc_stub_t stub) : id_(id), stub_(std::move(stub)) {}
  int64_t id_;
  rpc_stub_t stub_;
};

using rpc_server_t = std::unique_ptr<grpc::Server>;

class MultiPaxos : public multipaxos::MultiPaxosRPC::Service {
 public:
  MultiPaxos(Log* log, nlohmann::json const& config);
  MultiPaxos(MultiPaxos const& log) = delete;
  MultiPaxos& operator=(MultiPaxos const& log) = delete;
  MultiPaxos(MultiPaxos&& log) = delete;
  MultiPaxos& operator=(MultiPaxos&& log) = delete;

  int64_t NextBallot() {
    std::scoped_lock lock(mu_);
    ballot_ += kRoundIncrement;
    ballot_ = (ballot_ & ~kIdBits) | id_;
    ready_ = false;
    DLOG(INFO) << id_ << " became a leader";
    cv_leader_.notify_one();
    return ballot_;
  }

  void SetBallot(int64_t ballot) {
    if ((ballot_ & kIdBits) == id_ && (ballot & kIdBits) != id_) {
      DLOG(INFO) << id_ << " became a follower";
      cv_follower_.notify_one();
    }
    ballot_ = ballot;
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
  void Stop();

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
  bool ready_;
  int64_t ballot_;
  Log* log_;
  int64_t id_;
  long heartbeat_interval_;
  std::default_random_engine engine_;
  std::uniform_int_distribution<int> dist_;
  std::string port_;
  std::atomic<long> last_heartbeat_;
  std::vector<rpc_peer_t> rpc_peers_;
  rpc_server_t rpc_server_;
  mutable std::mutex mu_;
  asio::thread_pool thread_pool_;

  std::thread heartbeat_thread_;
  std::condition_variable cv_leader_;
  multipaxos::HeartbeatRequest heartbeat_request_;
  size_t heartbeat_num_rpcs_;
  std::vector<int64_t> heartbeat_responses_;
  std::mutex heartbeat_mu_;
  std::condition_variable heartbeat_cv_;

  std::thread prepare_thread_;
  std::condition_variable cv_follower_;
  multipaxos::PrepareRequest prepare_request_;
  size_t prepare_num_rpcs_;
  std::vector<log_vector_t> prepare_ok_responses_;
  std::mutex prepare_mu_;
  std::condition_variable prepare_cv_;
};

#endif
