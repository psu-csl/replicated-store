#ifndef CLIENT_MANAGER_H_
#define CLIENT_MANAGER_H_

#include <asio.hpp>

#include "client.h"
#include "multipaxos.h"

using client_ptr = std::shared_ptr<Client>;

class ClientManager {
 public:
  ClientManager(int64_t id, int64_t num_peers, MultiPaxos* multi_paxos)
      : next_client_id_(id), num_peers_(num_peers), multi_paxos_(multi_paxos) {}
  ClientManager(ClientManager const&) = delete;
  ClientManager& operator=(ClientManager const&) = delete;
  ClientManager(ClientManager const&&) = delete;
  ClientManager& operator=(ClientManager const&&) = delete;

  void Start(asio::ip::tcp::socket socket);
  void Respond(int64_t client_id, std::string const& response);
  void Stop(int64_t client_id);
  void StopAll();

 private:
  int64_t NextClientId() {
    auto id = next_client_id_;
    next_client_id_ += num_peers_;
    return id;
  }

  int64_t next_client_id_;
  int64_t num_peers_;
  MultiPaxos* multi_paxos_;
  std::unordered_map<int64_t, client_ptr> clients_;
};

#endif
