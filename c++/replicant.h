#ifndef REPLICANT_H_
#define REPLICANT_H_

#include <memory>
#include <optional>
#include <asio.hpp>

#include "memstore.h"
#include "paxos.h"

#define CLIENT_PORT 4444
#define PEER_PORT   7777

using asio::ip::tcp;

typedef std::shared_ptr<tcp::socket> socket_ptr;

class Replicant {
public:
  Replicant();
  ~Replicant();
  void Run();
private:
  void HandleClient(socket_ptr cli);
  void HandleCommand(socket_ptr cli, Command cmd);
  std::optional<Command> ReadCommand(socket_ptr cli);
  std::string ReadLine(socket_ptr cli);

  std::unique_ptr<Consensus> consensus_;
  asio::thread_pool tp_;
  asio::io_context io_;
  tcp::acceptor acceptor_;
};

#endif
