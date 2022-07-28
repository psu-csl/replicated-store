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
    : mp_(&log_, config), acceptor_(io_), tp_(config["threadpool_size"]) {
  int id = config["id"];
  std::string me = config["peers"][id];
  auto pos = me.find(":") + 1;
  CHECK_NE(pos, std::string::npos);
  std::string ip = me.substr(0, pos);
  int port = std::stoi(me.substr(pos)) + 1;

  tcp::endpoint endpoint(tcp::v4(), port);
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(tcp::acceptor::reuse_address(true));
  acceptor_.bind(endpoint);
  acceptor_.listen(5);
  DLOG(INFO) << "replicant " << id << " accepting at " << ip << ":" << port;
}

Replicant::~Replicant() {
  tp_.join();
}

void Replicant::Run() {
  for (;;) {
    tcp::socket cli(io_);
    acceptor_.accept(cli);
    std::thread(&Replicant::HandleClient, this, std::move(cli)).detach();
  }
}

void Replicant::HandleClient(tcp::socket cli) {
  for (;;) {
    auto cmd = ReadCommand(&cli);
    if (cmd)
      asio::post(tp_, [this, &cli, cmd = std::move(*cmd)] {
        HandleCommand(&cli, std::move(cmd));
      });
    else
      break;
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

void Replicant::HandleCommand(tcp::socket* /* cli */, Command /* command */) {}
