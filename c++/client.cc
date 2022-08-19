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
  std::istream request_stream(request);
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

void Client::HandleRequest() {
  auto self(shared_from_this());
  asio::async_read_until(
      socket_, request_, '\n', [this, self](std::error_code ec, size_t n) {
        if (!ec) {
          auto command = Parse(&request_);
          if (command) {
            auto r = multi_paxos_->Replicate(std::move(*command), id_);
            if (r.type_ == ResultType::kOk) {
              HandleRequest();
            } else if (r.type_ == ResultType::kRetry) {
              WriteResponse("retry");
            } else {
              CHECK(r.type_ == ResultType::kSomeoneElseLeader);
              WriteResponse("leader is ...");
            }
          } else {
            WriteResponse("bad command");
          }
        } else {
          manager_->Stop(id_);
        }
      });
}

void Client::WriteResponse(std::string const& response) {
  std::ostream response_stream(&response_);
  response_stream << response;

  auto self(shared_from_this());
  asio::async_write(socket_, response_,
                    [this, self, response](std::error_code ec, size_t n) {
                      if (ec) {
                        manager_->Stop(id_);
                      }
                      HandleRequest();
                    });
}
