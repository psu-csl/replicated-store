#include "replicant.h"

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

Replicant::Replicant(const json& config)
    : consensus_(new Paxos(new MemStore(), config)),
      acceptor_(io_),
      tp_(config["threadpool_size"]) {
  // determine port number for clients, which is 1 more than the port for paxos
  // peers; allows us to run multiple paxos peers on the same host for testing
  int id = config["id"];
  std::string me = config["peers"][id];
  auto pos = me.find(":") + 1;
  CHECK_NE(pos, std::string::npos);
  std::string ip = me.substr(0, pos);
  int port = std::stoi(me.substr(pos)) + 1;

  // start the server for accepting client commands
  tcp::endpoint endpoint(tcp::v4(), port);
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(tcp::acceptor::reuse_address(true));
  acceptor_.bind(endpoint);
  acceptor_.listen(5);
  DLOG(INFO) << "accepting clients at " << ip << ":" << port;
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
  if (strncmp(line.c_str(), "get", 3) == 0)
    return Command{CommandType::kGet, line.substr(4), ""};
  if (strncmp(line.c_str(), "del", 3) == 0)
    return Command{CommandType::kDel, line.substr(4), ""};

  CHECK(strncmp(line.c_str(), "put", 3));
  size_t p = line.find(":", 4);
  return Command{CommandType::kPut, line.substr(4, p - 4), line.substr(p + 1)};
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

void Replicant::HandleCommand(tcp::socket* cli, Command cmd) {
  bool is_get = cmd.type == CommandType::kGet;
  auto r = consensus_->AgreeAndExecute(std::move(cmd));

  static const std::string success = "success\n";
  static const std::string failure = "failure\n";

  if (r.ok) {
    asio::write(*cli, asio::buffer(success));
    if (is_get) {
      r.value.push_back('\n');
      asio::write(*cli, asio::buffer(r.value));
    }
  } else {
    asio::write(*cli, asio::buffer(failure));
  }
}
