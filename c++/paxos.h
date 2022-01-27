#ifndef PAXOS_H_
#define PAXOS_H_

#include <grpcpp/grpcpp.h>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>
#include "consensus.h"
#include "json_fwd.h"
#include "kvstore.h"
#include "paxosrpc.h"

using grpc::ServerBuilder;
using nlohmann::json;

class Paxos : public Consensus {
 public:
  Paxos(const json& config, KVStore* store);
  ~Paxos() = default;
  Result AgreeAndExecute(Command command) override;

  void set_min_last_executed(uint32_t n) {
    std::lock_guard lock(mu_);
    min_last_executed_ = n;
  }

  uint32_t last_executed() const {
    std::lock_guard lock(mu_);
    return last_executed_;
  }

 private:
  void HeartBeat(void);

  uint32_t id_;
  bool leader_;
  uint32_t last_executed_;
  uint32_t min_last_executed_;

  std::condition_variable cv_;
  mutable std::mutex mu_;
  std::unique_ptr<KVStore> store_;

  // RPC stuff
  std::vector<PaxosRPCClient> rpc_peers_;
  PaxosRPCServiceImpl rpc_server_;
  ServerBuilder builder_;
};

#endif
