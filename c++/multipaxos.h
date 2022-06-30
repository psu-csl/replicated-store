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

using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using multipaxosrpc::HeartbeatRequest;
using multipaxosrpc::HeartbeatResponse;
using multipaxosrpc::MultiPaxosRPC;

using nlohmann::json;

static const int64_t kIdBits = 0xff;
static const int64_t kRoundIncrement = kIdBits + 1;
static const int64_t kMaxNumPeers = 0xf;

class MultiPaxos : public MultiPaxosRPC::Service {
 public:
  MultiPaxos(Log* log, json const& config);
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

  void Start();
  void Shutdown();
  void HeartbeatThread();

 private:
  Status Heartbeat(ServerContext*,
                   const HeartbeatRequest*,
                   HeartbeatResponse*) override;

  std::atomic<bool> running_;
  int64_t id_;
  std::string port_;
  int64_t ballot_;
  std::chrono::time_point<std::chrono::steady_clock> last_heartbeat_;
  std::chrono::milliseconds heartbeat_pause_;
  Log* log_;
  std::vector<std::unique_ptr<MultiPaxosRPC::Stub>> rpc_peers_;
  std::unique_ptr<Server> rpc_server_;
  mutable std::mutex mu_;
  std::condition_variable cv_leader_;
  asio::thread_pool tp_;
  std::thread heartbeat_thread_;

  std::mutex heartbeat_mu_;
  std::condition_variable heartbeat_cv_;
  size_t heartbeat_num_responses_;
  std::vector<int64_t> heartbeat_ok_responses_;
  HeartbeatRequest heartbeat_request_;
};

#endif
