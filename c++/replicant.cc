#include <glog/logging.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <asio.hpp>
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
  executor_thread_ = std::thread(&Replicant::ExecutorThread, this);
  for (;;) {
    auto client_id = NextClientId();
    auto [it, ok] = client_sockets_.insert({client_id, tcp::socket(io_)});
    CHECK(ok);
    acceptor_.accept(it->second);
    clients_.emplace_back(&Replicant::HandleClient, this, client_id);
  }
}

void Replicant::Stop() {
  tp_.join();
  for (auto& t : clients_)
    t.join();
  executor_thread_.join();
  mp_.Stop();
}

void Replicant::HandleClient(int64_t client_id) {
  auto it = client_sockets_.find(client_id);
  CHECK(it != client_sockets_.end());
  for (;;) {
    auto command = ReadCommand(&it->second);
    if (command)
      asio::post(tp_, [this, command = std::move(*command), client_id] {
        Replicate(std::move(command), client_id);
      });
    else
      break;
  }
}

void Replicant::ExecutorThread() {
  DLOG(INFO) << "replicant " << id_ << " starting executor thread";
  for (;;) {
    auto [client_id, result] = log_.Execute(kv_store_.get());
    auto it = client_sockets_.find(client_id);
    if (it == client_sockets_.end())
      return;
    asio::error_code ec;
    if (!result.ok_)
      asio::write(it->second, asio::buffer("failed"), ec);
    asio::write(it->second, asio::buffer(*result.value_), ec);
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

void Replicant::Replicate(multipaxos::Command command, int64_t client_id) {
  auto r = mp_.Replicate(std::move(command), client_id);
  if (r.type_ == ResultType::kOk)
    return;

  auto it = client_sockets_.find(client_id);
  CHECK(it != client_sockets_.end());

  if (r.type_ == ResultType::kRetry) {
    // write to socket (it->second) a retry message
  } else {
    CHECK(r.type_ == ResultType::kSomeoneElseLeader);
    // write to socket (it->second) leader's id
    it->second.close();
    client_sockets_.erase(it);
  }
}
