#ifndef REPLICANT_H_
#define REPLICANT_H_

#include <asio.hpp>
#include <memory>
#include <optional>

#include "client_manager.h"
#include "json_fwd.h"
#include "log.h"
#include "memkvstore.h"
#include "multipaxos.h"

class Replicant {
 public:
  Replicant(const nlohmann::json& config);
  void Start();
  void Stop();

 private:
  void StartServer();
  void StopServer();

  void StartExecutorThread();
  void StopExecutorThread();

  void ExecutorThread();

  void AcceptClient();

  int64_t id_;
  int64_t num_peers_;
  Log log_;
  MultiPaxos mp_;
  std::unique_ptr<KVStore> kv_store_;
  std::string ip_port_;
  asio::io_context io_;
  asio::ip::tcp::acceptor acceptor_;
  ClientManager client_manager_;
  std::thread executor_thread_;
};

#endif
