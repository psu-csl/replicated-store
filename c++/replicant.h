#ifndef REPLICANT_H_
#define REPLICANT_H_

#include <asio.hpp>
#include <memory>
#include <optional>

#include "json_fwd.h"
#include "log.h"
#include "memkvstore.h"
#include "multipaxos.h"

using asio::ip::tcp;
using nlohmann::json;

class Replicant {
 public:
  Replicant(const json& config);
  ~Replicant();
  void Run();

 private:
  void HandleClient(tcp::socket cli);
  void HandleCommand(tcp::socket* cli, Command cmd);
  std::optional<Command> ReadCommand(tcp::socket* cli);
  std::string ReadLine(tcp::socket* cli);

  Log log_;
  MultiPaxos mp_;
  asio::io_context io_;
  tcp::acceptor acceptor_;
  asio::thread_pool tp_;
};

#endif
