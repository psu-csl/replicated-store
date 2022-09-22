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

class Replicant : public std::enable_shared_from_this<Replicant> {
 public:
  Replicant(asio::io_context* io_context, nlohmann::json const& config);
  Replicant(Replicant const& replicant) = delete;
  Replicant& operator=(Replicant const& replicant) = delete;
  Replicant(Replicant&& replicant) = delete;
  Replicant& operator=(Replicant&& replicant) = delete;

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
  MultiPaxos multi_paxos_;
  std::string ip_port_;
  asio::io_context* io_context_;
  asio::ip::tcp::acceptor acceptor_;
  ClientManager client_manager_;
  std::thread executor_thread_;
};

#endif
