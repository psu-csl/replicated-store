#ifndef CLIENT_H_
#define CLIENT_H_

#include <asio.hpp>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "json_fwd.h"
#include "msg.h"

using nlohmann::json;

class MultiPaxos;
class ClientManager;

class Client : public std::enable_shared_from_this<Client> {
 public:
  Client(int64_t id,
         asio::ip::tcp::socket socket,
         MultiPaxos* multi_paxos,
         ClientManager* manager,
         bool is_from_client)
         //asio::thread_pool& thread_pool)
      : id_(id),
        socket_(std::move(socket)),
        multi_paxos_(multi_paxos),
        manager_(manager),
        is_from_client_(is_from_client) {}
        //thread_pool_(thread_pool) {}
  Client(Client const&) = delete;
  Client& operator=(Client const&) = delete;
  Client(Client const&&) = delete;
  Client& operator=(Client const&&) = delete;

  void Start();
  void Stop();

  void Read();
  void Write(std::string const& response);
  void HandleClientRequest();
  void HandlePeerRequest();

 private:
  int64_t id_;
  asio::ip::tcp::socket socket_;
  asio::streambuf request_;
  asio::streambuf response_;
  MultiPaxos* multi_paxos_;
  ClientManager* manager_;
  bool is_from_client_;
  //std::mutex writer_mu_;
  //asio::thread_pool& thread_pool_;
};

#endif
