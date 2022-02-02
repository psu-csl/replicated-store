#ifndef PAXOS_H_
#define PAXOS_H_

#include <grpcpp/grpcpp.h>
#include <asio.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>
#include "consensus.h"
#include "json_fwd.h"
#include "kvstore.h"
#include "paxosrpc.h"

using grpc::Server;
using grpc::ServerBuilder;
using nlohmann::json;

class Paxos : public Consensus {
 public:
  Paxos(KVStore* store, const json& config);
  ~Paxos() = default;
  Result AgreeAndExecute(Command command) override;

  void set_min_last_executed(uint32_t n) {
    std::lock_guard lock(mu_);
    min_last_executed_ = n;
  }

  int id(void) const { return id_; }

  uint32_t min_last_executed(void) const {
    std::lock_guard lock(mu_);
    return min_last_executed_;
  }

  uint32_t last_executed(void) const {
    std::lock_guard lock(mu_);
    return last_executed_;
  }

 private:
  void HeartBeat(void);

  uint32_t id_;
  bool leader_;
  std::chrono::milliseconds heartbeat_pause_;
  uint32_t last_executed_ = 0;
  uint32_t min_last_executed_ = 0;
  asio::thread_pool tp_;
  std::condition_variable cv_;
  mutable std::mutex mu_;
  std::unique_ptr<KVStore> store_;

  std::vector<PaxosRPCClient> rpc_peers_;
  PaxosRPCServiceImpl rpc_service_;
  std::unique_ptr<Server> rpc_server_;

  asio::thread_pool tp_;
};

#endif
