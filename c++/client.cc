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

void Client::Start() {
  auto self(shared_from_this());
  asio::dispatch(socket_.get_executor(), [this, self] { Read(); });
}

void Client::Stop() {
  auto self(shared_from_this());
  asio::dispatch(socket_.get_executor(), [this, self] { socket_.close(); });
}

void Client::Read() {
  auto self(shared_from_this());
  asio::async_read_until(
      socket_, request_, '\n', [this, self](std::error_code ec, size_t) {
        if (!ec) {
          auto command = Parse(&request_);
          if (command) {
            auto r = multi_paxos_->Replicate(std::move(*command), id_);
            if (r.type_ == ResultType::kOk)
              return;
            if (r.type_ == ResultType::kRetry) {
              Write("retry");
            } else {
              CHECK(r.type_ == ResultType::kSomeoneElseLeader);
              Write("leader is " + std::to_string(*r.leader_));
            }
          } else {
            Write("bad command");
          }
        } else if (ec != asio::error::operation_aborted) {
          manager_->Stop(id_);
        }
      });
}

void Client::Write(std::string const& response) {
  std::ostream response_stream(&response_);
  response_stream << response << '\n';

  auto self(shared_from_this());
  asio::async_write(socket_, response_,
                    [this, self, response](std::error_code ec, size_t) {
                      if (ec) {
                        manager_->Stop(id_);
                      }
                      Read();
                    });
}
