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
  void HandleClient(asio::ip::tcp::socket cli);
  void HandleCommand(asio::ip::tcp::socket* cli, multipaxos::Command cmd);
  std::optional<multipaxos::Command> ReadCommand(asio::ip::tcp::socket* cli);
  std::string ReadLine(asio::ip::tcp::socket* cli);

  Log log_;
  MultiPaxos mp_;
  asio::io_context io_;
  asio::ip::tcp::acceptor acceptor_;
  asio::thread_pool tp_;
  std::vector<std::thread> clients_;
};

#endif
