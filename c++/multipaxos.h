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
    cv_leader_.notify_all();
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

  void Start();
  void Shutdown();
  void HeartbeatThread();

 private:
  grpc::Status Heartbeat(grpc::ServerContext*,
                         const multipaxos::HeartbeatRequest*,
                         multipaxos::HeartbeatResponse*) override;

  grpc::Status Prepare(grpc::ServerContext*,
                       const multipaxos::PrepareRequest*,
                       multipaxos::PrepareResponse*) override;

  std::atomic<bool> running_;
  int64_t id_;
  std::string port_;
  int64_t ballot_;
  std::chrono::time_point<std::chrono::steady_clock> last_heartbeat_;
  std::chrono::milliseconds heartbeat_pause_;
  Log* log_;
  std::vector<std::unique_ptr<multipaxos::MultiPaxosRPC::Stub>> rpc_peers_;
  std::unique_ptr<grpc::Server> rpc_server_;
  mutable std::mutex mu_;
  std::condition_variable cv_leader_;
  asio::thread_pool tp_;
  std::thread heartbeat_thread_;

  multipaxos::HeartbeatRequest heartbeat_request_;
  size_t heartbeat_num_responses_;
  std::vector<int64_t> heartbeat_ok_responses_;
  std::mutex heartbeat_mu_;
  std::condition_variable heartbeat_cv_;
};

#endif
