#include "peer_server.h"

namespace server {
void CreateListener(int port, asio::ip::tcp::acceptor& acceptor) {
  tcp::endpoint endpoint(tcp::v4(), port);
  acceptor.open(endpoint.protocol());
  acceptor.set_option(tcp::acceptor::reuse_address(true));
  acceptor.set_option(tcp::no_delay(true));
  acceptor.bind(endpoint);
  acceptor.listen(5);
}

void AcceptClient(asio::io_context* io_context, 
                  ClientManager* client_manager, 
                  asio::ip::tcp::acceptor* acceptor) {
  acceptor->async_accept(asio::make_strand(*io_context),
                         [io_context, client_manager, acceptor]
                             (std::error_code ec, tcp::socket socket) {
                           if (!acceptor->is_open()) {
                             return;
                           }
                           if (!ec) {
                             client_manager->Start(std::move(socket));
                             AcceptClient(io_context, client_manager, acceptor);
                           }
                         });
}

} // namespace server

void PeerServer::StartServer(MultiPaxos* multi_paxos) {
  auto pos = ip_port_.find(":") + 1;
  CHECK_NE(pos, std::string::npos);
  int port = std::stoi(ip_port_.substr(pos));
  server::CreateListener(port, acceptor_);
  DLOG(INFO) << id_ << " starting peer server at port " << port;

  multi_paxos_ = multi_paxos;
  peer_manager_ = new ClientManager(id_, num_peers_, multi_paxos_, 
  	                                false, threadpool_size_);
  auto self(shared_from_this());
  asio::dispatch(acceptor_.get_executor(), [this, self] { 
    server::AcceptClient(io_context_, peer_manager_, &acceptor_); 
  });
}

void PeerServer::StopServer() {
  auto self(shared_from_this());
  asio::post(acceptor_.get_executor(), [this, self] {
    if (acceptor_.is_open())
      acceptor_.close();
  });
  peer_manager_->StopAll();
}
