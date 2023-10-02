#ifndef PEER_SERVER_H
#define PEER_SERVER_H

#include <asio.hpp>
#include <memory>

#include "client_manager.h"

namespace server {
  void CreateListener(int port, asio::ip::tcp::acceptor& acceptor);
  void AcceptClient(asio::io_context* io_context, 
                  ClientManager* client_manager, 
                  asio::ip::tcp::acceptor* acceptor);
}

class PeerServer : public std::enable_shared_from_this<PeerServer> {
 public:
  PeerServer(int64_t id, 
  	         std::string ip_port, 
  	         asio::io_context* io_context,
  	         int num_peers,
  	         int threadpool_size)
      : id_(id), 
        ip_port_(ip_port), 
        io_context_(io_context), 
        num_peers_(num_peers),
        acceptor_(asio::make_strand(*io_context_)),
        threadpool_size_(threadpool_size) { 
    
  }
  
  void StartServer(MultiPaxos* multi_paxos);

  void StopServer();

 private:
  int64_t id_;
  MultiPaxos* multi_paxos_;
  std::string ip_port_;
  asio::io_context* io_context_;
  int num_peers_;
  asio::ip::tcp::acceptor acceptor_;
  int threadpool_size_;
  ClientManager* peer_manager_;
};

#endif