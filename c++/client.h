#ifndef CLIENT_H_
#define CLIENT_H_

#include <asio.hpp>
#include <memory>
#include <unordered_map>

class MultiPaxos;
class ClientManager;

class Client : public std::enable_shared_from_this<Client> {
 public:
  Client(int64_t id,
         asio::ip::tcp::socket socket,
         MultiPaxos* multi_paxos,
         ClientManager* manager)
      : id_(id),
        socket_(std::move(socket)),
        multi_paxos_(multi_paxos),
        manager_(manager) {}
  Client(Client const&) = delete;
  Client& operator=(Client const&) = delete;
  Client(Client const&&) = delete;
  Client& operator=(Client const&&) = delete;

  void Start() { HandleRequest(); }
  void Stop() { socket_.close(); }

  void HandleRequest();
  void WriteResponse(std::string const& response);

 private:
  int64_t id_;
  asio::ip::tcp::socket socket_;
  asio::streambuf request_;
  asio::streambuf response_;
  MultiPaxos* multi_paxos_;
  ClientManager* manager_;
};

#endif
