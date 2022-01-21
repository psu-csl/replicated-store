#include "replicant.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <asio.hpp>
#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>

const int kClientPort = 4444;

Replicant::Replicant()
    : consensus_(new Paxos(new MemStore())),
      num_threads_(std::thread::hardware_concurrency()),
      tp_(num_threads_),
      acceptor_(io_, tcp::endpoint(tcp::v4(), kClientPort)) {
  acceptor_.listen(5);
  assert(num_threads_);
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

  assert(strncmp(line.c_str(), "put", 3) == 0);
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
