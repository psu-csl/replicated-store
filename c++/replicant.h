#ifndef REPLICANT_H_
#define REPLICANT_H_

#include <asio.hpp>
#include <memory>
#include <optional>

#include "json_fwd.h"
#include "log.h"
#include "memkvstore.h"
#include "multipaxos.h"

class Replicant {
 public:
  Replicant(const nlohmann::json& config);
  ~Replicant();
  void Run();

 private:
  void HandleClient(int64_t client_id);
  void Replicate(multipaxos::Command command, int64_t client_id);
  void ExecutorThread();

  std::optional<multipaxos::Command> ReadCommand(asio::ip::tcp::socket* cli);
  std::string ReadLine(asio::ip::tcp::socket* cli);

  int64_t NextClientId() {
    auto id = next_client_id_;
    next_client_id_ += num_peers_;
    return id;
  }

  Log log_;
  MultiPaxos mp_;
  std::unique_ptr<KVStore> kv_store_;
  int64_t id_;
  int64_t next_client_id_;
  int64_t num_peers_;
  std::unordered_map<int64_t, asio::ip::tcp::socket> client_sockets_;
  asio::io_context io_;
  asio::ip::tcp::acceptor acceptor_;
  asio::thread_pool tp_;
  std::vector<std::thread> clients_;
  std::thread executor_thread_;
};

#endif
