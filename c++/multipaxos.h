#ifndef MULTI_PAXOS_H_
#define MULTI_PAXOS_H_

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>

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

  int64_t id() const { return id_; }

  int64_t NextBallot() {
    std::scoped_lock lock(mu_);
    ballot_ += kRoundIncrement;
    ballot_ = (ballot_ & ~kIdBits) | id_;
    return ballot_;
  }

  int64_t Leader() const {
    std::scoped_lock lock(mu_);
    return ballot_ & kIdBits;
  }

  bool IsLeader() const {
    std::scoped_lock lock(mu_);
    return (ballot_ & kIdBits) == id_;
  }

  bool IsSomeoneElseLeader() const {
    auto id = Leader();
    return id != id_ && id < kMaxNumPeers;
  }

  void Start();
  void Shutdown();

 private:
  Status Heartbeat(ServerContext*,
                   const HeartbeatRequest*,
                   HeartbeatResponse*) override;

  int64_t id_;
  std::string port_;
  int64_t ballot_;
  std::chrono::time_point<std::chrono::steady_clock> last_heartbeat_;
  Log* log_;
  std::unique_ptr<Server> server_;
  mutable std::mutex mu_;
};

#endif
