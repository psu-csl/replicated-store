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
      log_(kvstore::CreateStore(config)),
      multi_paxos_(&log_, config, io_context),
      ip_port_(config["peers"][id_]),
      io_context_(io_context),
      acceptor_(asio::make_strand(*io_context_)),
      client_manager_(id_, config["peers"].size(), &multi_paxos_,
                      true, config["threadpool_size"]),
      peer_server_(id_, ip_port_, io_context_,
                   config["peers"].size(), config["threadpool_size"]) {}

void Replicant::Start() {
  peer_server_.StartServer(&multi_paxos_);
  multi_paxos_.Start();
  StartExecutorThread();
  StartServer();
}

void Replicant::Stop() {
  peer_server_.StopServer();
  StopServer();
  StopExecutorThread();
  multi_paxos_.Stop();
}

void Replicant::StartServer() {
  auto pos = ip_port_.find(":") + 1;
  CHECK_NE(pos, std::string::npos);
  int port = std::stoi(ip_port_.substr(pos)) + 1;

  server::CreateListener(port, acceptor_);
  DLOG(INFO) << id_ << " starting server at port " << port;

  auto self(shared_from_this());
  asio::dispatch(acceptor_.get_executor(), [this, self] { 
    server::AcceptClient(io_context_, &client_manager_, &acceptor_); 
  });
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
