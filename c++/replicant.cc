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

Replicant::Replicant(json const& config)
    : id_(config["id"]),
      num_peers_(config["peers"].size()),
      multi_paxos_(&log_, config),
      kv_store_(std::make_unique<MemKVStore>()),
      ip_port_(config["peers"][id_]),
      acceptor_(io_),
      client_manager_(id_, num_peers_, &multi_paxos_) {}

void Replicant::Start() {
  multi_paxos_.Start();
  StartExecutorThread();
  StartServer();
  io_.run();
}

void Replicant::Stop() {
  StopExecutorThread();
  StopServer();
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

  AcceptClient();
}

void Replicant::StopServer() {
  acceptor_.close();
  client_manager_.StopAll();
}

void Replicant::AcceptClient() {
  acceptor_.async_accept([this](std::error_code ec, tcp::socket socket) {
    if (!acceptor_.is_open())
      return;
    CHECK(!ec);
    client_manager_.Start(std::move(socket));
    AcceptClient();
  });
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
    auto r = log_.Execute(kv_store_.get());
    if (!r)
      break;
    auto [client_id, result] = *r;
    client_manager_.Respond(client_id, *result.value_);
  }
}
