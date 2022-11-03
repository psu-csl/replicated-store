#include <glog/logging.h>
#include <asio.hpp>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>

#include "json.h"
#include "replicant.h"

using asio::ip::tcp;
using nlohmann::json;

Replicant::Replicant(asio::io_context* io_context, json const& config)
    : id_(config["id"]),
      log_(std::make_unique<kvstore::MemKVStore>()),
      multi_paxos_(&log_, config),
      ip_port_(config["peers"][id_]),
      io_context_(io_context),
      acceptor_(asio::make_strand(*io_context_)),
      client_manager_(id_, config["peers"].size(), &multi_paxos_) {}

void Replicant::Start() {
  multi_paxos_.Start();
  StartExecutorThread();
  StartServer();
}

void Replicant::Stop() {
  StopServer();
  StopExecutorThread();
  multi_paxos_.Stop();
}

void Replicant::StartServer() {
  auto pos = ip_port_.find(":") + 1;
  CHECK_NE(pos, std::string::npos);
  int port = std::stoi(ip_port_.substr(pos)) + 1;

  tcp::endpoint endpoint(tcp::v4(), port);
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(tcp::acceptor::reuse_address(true));
  acceptor_.bind(endpoint);
  acceptor_.listen(5);
  DLOG(INFO) << id_ << " starting server at port " << port;

  auto self(shared_from_this());
  asio::dispatch(acceptor_.get_executor(), [this, self] { AcceptClient(); });
}

void Replicant::StopServer() {
  auto self(shared_from_this());
  asio::post(acceptor_.get_executor(), [this, self] { acceptor_.close(); });
  client_manager_.StopAll();
}

void Replicant::StartExecutorThread() {
  DLOG(INFO) << id_ << " starting executor thread";
  executor_thread_ = std::thread(&Replicant::ExecutorThread, this);
}

void Replicant::StopExecutorThread() {
  DLOG(INFO) << id_ << " stopping executor thread";
  log_.Stop();
  executor_thread_.join();
}

void Replicant::ExecutorThread() {
  for (;;) {
    auto r = log_.Execute();
    if (!r)
      break;
    auto [id, result] = std::move(*r);
    auto client = client_manager_.Get(id);
    if (client)
      client->Write(result.value_);
  }
}

void Replicant::AcceptClient() {
  auto self(shared_from_this());
  acceptor_.async_accept(asio::make_strand(*io_context_),
                         [this, self](std::error_code ec, tcp::socket socket) {
                           if (!acceptor_.is_open())
                             return;
                           if (!ec) {
                             client_manager_.Start(std::move(socket));
                             AcceptClient();
                           }
                         });
}
