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
  void Start();
  void Stop();

 private:
  void HandleClient(int64_t client_id, asio::ip::tcp::socket* socket);
  void Replicate(multipaxos::Command command,
                 int64_t client_id,
                 asio::ip::tcp::socket* socket);

  void StartExecutorThread();
  void StopExecutorThread();

  void ExecutorThread();

  std::optional<multipaxos::Command> ReadCommand(asio::ip::tcp::socket* cli);
  std::string ReadLine(asio::ip::tcp::socket* cli);

  int64_t NextClientId() {
    auto id = next_client_id_;
    next_client_id_ += num_peers_;
    return id;
  }

  asio::ip::tcp::socket* CreateSocket(int64_t client_id) {
    std::scoped_lock lock(mu_);
    auto [it, ok] =
        client_sockets_.insert({client_id, asio::ip::tcp::socket(io_)});
    CHECK(ok);
    return &it->second;
  }

  asio::ip::tcp::socket* FindSocket(int64_t client_id) {
    std::scoped_lock lock(mu_);
    auto it = client_sockets_.find(client_id);
    if (it == client_sockets_.end())
      return nullptr;
    return &it->second;
  }

  void RemoveSocket(int64_t client_id) {
    std::scoped_lock lock(mu_);
    auto ok = client_sockets_.erase(client_id);
    CHECK(ok);
  }

  Log log_;
  MultiPaxos mp_;
  std::unique_ptr<KVStore> kv_store_;
  int64_t id_;
  int64_t next_client_id_;
  int64_t num_peers_;
  std::mutex mu_;
  std::unordered_map<int64_t, asio::ip::tcp::socket> client_sockets_;
  asio::io_context io_;
  asio::ip::tcp::acceptor acceptor_;
  asio::thread_pool tp_;
  std::vector<std::thread> client_threads_;
  std::thread executor_thread_;
};

#endif
