#include <istream>
#include <optional>

#include "client.h"
#include "client_manager.h"
#include "multipaxos.h"

using asio::ip::tcp;
using multipaxos::Command;
using multipaxos::CommandType::DEL;
using multipaxos::CommandType::GET;
using multipaxos::CommandType::PUT;

std::optional<Command> Parse(asio::streambuf* request) {
  std::string line;
  std::getline(std::istream(request), line);
  std::istringstream request_stream(line);
  std::string command, key;
  Command c;

  request_stream >> command;
  request_stream >> key;

  if (!request_stream)
    return std::nullopt;
  c.set_key(std::move(key));

  if (command == "get") {
    c.set_type(GET);
  } else if (command == "del") {
    c.set_type(DEL);
  } else if (command == "put") {
    c.set_type(PUT);
    std::string value;
    request_stream >> value;
    if (!request_stream)
      return std::nullopt;
    c.set_value(value);
  } else {
    return std::nullopt;
  }
  return c;
}

asio::awaitable<void> Client::Start() {
  co_await Read();
}

void Client::Stop() {
  socket_.close();
}

asio::awaitable<void> Client::Read() {
  try {
    while (socket_.is_open()) {
      auto n = co_await asio::async_read_until(
        socket_, request_, '\n', asio::use_awaitable);
      auto command = Parse(&request_);
      if (command) {
        auto r = co_await multi_paxos_->Replicate(std::move(*command), id_);
        if (r.type_ == ResultType::kOk)
          continue;
        if (r.type_ == ResultType::kRetry) {
          Write("retry");
        } else {
          CHECK(r.type_ == ResultType::kSomeoneElseLeader);
          Write("leader is " + std::to_string(*r.leader_));
        }
      } else {
        Write("bad command");
      }
    }
  } catch (std::exception& e) {
    manager_->Stop(id_);
  }
}

asio::awaitable<void> Client::Write(std::string const& response) {
  std::ostream response_stream(&response_);
  response_stream << response << '\n';
  DLOG(INFO) << "ready to write";
  auto n = co_await asio::async_write(socket_, response_, asio::use_awaitable);
  DLOG(INFO) << "write: " << n;
}
