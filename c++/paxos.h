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

 private:
  void HeartBeat(void);

  uint32_t id_;
  bool leader_;
  std::condition_variable cv_;
  std::mutex mu_;
  std::unique_ptr<KVStore> store_;

  // // read/updated by the heartbeat thread only; no need for synchronization.
  // int min_last_executed_;

  // RPC stuff
  std::vector<PaxosRPCClient> rpc_clients_;
  PaxosRPCServiceImpl rpc_server_;
  ServerBuilder builder_;
};

#endif
