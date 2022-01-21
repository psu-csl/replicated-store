#ifndef REPLICANT_H_
#define REPLICANT_H_

#include <asio.hpp>
#include <memory>
#include <optional>

#include "memstore.h"
#include "paxos.h"

using asio::ip::tcp;

class Replicant {
 public:
  Replicant();
  ~Replicant();
  void Run();

 private:
  void HandleClient(tcp::socket cli);
  void HandleCommand(tcp::socket* cli, Command cmd);
  std::optional<Command> ReadCommand(tcp::socket* cli);
  std::string ReadLine(tcp::socket* cli);

  std::unique_ptr<Consensus> consensus_;
  asio::io_context io_;
  unsigned int num_threads_;
  asio::thread_pool tp_;
  tcp::acceptor acceptor_;
};

#endif
