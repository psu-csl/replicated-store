#ifndef CLIENT_MANAGER_H_
#define CLIENT_MANAGER_H_

#include <asio.hpp>

#include "client.h"
#include "multipaxos.h"

using client_ptr = std::shared_ptr<Client>;

class ClientManager {
 public:
  ClientManager(int64_t id, 
                int64_t num_peers,
                MultiPaxos* multi_paxos,
                bool is_from_client,
                size_t thread_pool_size)
             : next_id_(id), 
               num_peers_(num_peers), 
               multi_paxos_(multi_paxos), 
               is_from_client_(is_from_client) {}
               //thread_pool_(thread_pool_size) {}
  ClientManager(ClientManager const&) = delete;
  ClientManager& operator=(ClientManager const&) = delete;
  ClientManager(ClientManager const&&) = delete;
  ClientManager& operator=(ClientManager const&&) = delete;

  void Start(asio::ip::tcp::socket socket);
  void Stop(int64_t id);
  void StopAll();

  client_ptr Get(int64_t id);

 private:
  int64_t NextClientId() {
    auto id = next_id_;
    next_id_ += num_peers_;
    return id;
  }

  int64_t next_id_;
  int64_t num_peers_;
  MultiPaxos* multi_paxos_;
  std::mutex mu_;
  std::unordered_map<int64_t, client_ptr> clients_;
  bool is_from_client_;
  //asio::thread_pool thread_pool_;
};

#endif
