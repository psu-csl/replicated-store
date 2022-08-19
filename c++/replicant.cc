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

using multipaxos::Command;
using multipaxos::CommandType::DEL;
using multipaxos::CommandType::GET;
using multipaxos::CommandType::PUT;

Replicant::Replicant(json const& config)
    : mp_(&log_, config),
      kv_store_(std::make_unique<MemKVStore>()),
      id_(config["id"]),
      next_client_id_(id_),
      num_peers_(config["peers"].size()),
      acceptor_(io_),
      tp_(config["threadpool_size"]) {
  std::string me = config["peers"][id_];
  auto pos = me.find(":") + 1;
  CHECK_NE(pos, std::string::npos);
  std::string ip = me.substr(0, pos);
  int port = std::stoi(me.substr(pos)) + 1;

  tcp::endpoint endpoint(tcp::v4(), port);
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(tcp::acceptor::reuse_address(true));
  acceptor_.bind(endpoint);
  acceptor_.listen(5);
  DLOG(INFO) << "replicant " << id_ << " accepting at " << ip << ":" << port;
}

void Replicant::Start() {
  mp_.Start();
  StartExecutorThread();
  for (;;) {
    auto client_id = NextClientId();
    auto socket = CreateSocket(client_id);
    asio::error_code ec;
    acceptor_.accept(*socket, ec);
    if (ec)
      break;
    client_threads_.emplace_back(&Replicant::HandleClient, this, client_id,
                                 socket);
  }
}

void Replicant::Stop() {
  for (auto& t : client_threads_)
    t.join();
  tp_.join();
  StopExecutorThread();
  acceptor_.close();
  mp_.Stop();
}

void Replicant::HandleClient(int64_t client_id, tcp::socket* socket) {
  for (;;) {
    auto command = ReadCommand(socket);
    if (command)
      asio::post(tp_, [this, command = std::move(*command), client_id, socket] {
        Replicate(std::move(command), client_id, socket);
      });
    else
      break;
  }
  socket->close();
  RemoveSocket(client_id);
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
    auto socket = FindSocket(client_id);
    if (!socket)
      continue;
    asio::error_code ec;
    if (!result.ok_)
      asio::write(*socket, asio::buffer("failed"), ec);
    asio::write(*socket, asio::buffer(*result.value_), ec);
  }
}

std::optional<Command> Replicant::ReadCommand(tcp::socket* cli) {
  std::string line = ReadLine(cli);
  if (line.empty())
    return std::nullopt;

  Command command;
  if (strncmp(line.c_str(), "get", 3) == 0) {
    command.set_type(GET);
    command.set_key(line.substr(4));
  } else if (strncmp(line.c_str(), "del", 3) == 0) {
    command.set_type(DEL);
    command.set_key(line.substr(4));
  } else {
    CHECK(strncmp(line.c_str(), "put", 3) == 0);
    size_t p = line.find(":", 4);
    command.set_type(PUT);
    command.set_key(line.substr(4, p - 4));
    command.set_value(line.substr(p + 1));
  }
  return command;
}

std::string Replicant::ReadLine(tcp::socket* cli) {
  std::string line;
  asio::streambuf request;
  asio::error_code ec;
  asio::read_until(*cli, request, '\n', ec);
  if (ec.value() == 0)
    std::getline(std::istream(&request), line);
  return line;
}

void Replicant::Replicate(multipaxos::Command command,
                          int64_t client_id,
                          tcp::socket* socket) {
  auto r = mp_.Replicate(std::move(command), client_id);
  if (r.type_ == ResultType::kOk)
    return;

  asio::error_code ec;
  if (r.type_ == ResultType::kRetry) {
    asio::write(*socket, asio::buffer("retry"), ec);
  } else {
    CHECK(r.type_ == ResultType::kSomeoneElseLeader);
    // TODO: write the leader's id
    asio::write(*socket, asio::buffer("the leader is ..."), ec);
  }
}
