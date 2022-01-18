#ifndef REPLICANT_H_
#define REPLICANT_H_

#include <optional>
#include <asio.hpp>

#include "memstore.h"
#include "paxos.h"
#include "threadpool.h"

#define CLIENT_PORT 4444
#define PEER_PORT   7777

class Replicant {
public:
  Replicant();
  ~Replicant();
  void Run();
private:
  void HandleClient(int fd);
  void HandleCommand(int fd, Command cmd);
  std::optional<Command> ReadCommand(int fd);
  std::string ReadLine(int fd);

  std::unique_ptr<Consensus> consensus_;
  asio::thread_pool tp_;
  int client_fd_;
  bool done_;
};

#endif
