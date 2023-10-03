#include <istream>
#include <optional>

#include "client.h"
#include "client_manager.h"
#include "multipaxos.h"

using asio::ip::tcp;
using CommandType::DEL;
using CommandType::GET;
using CommandType::PUT;

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
  c.key_ = std::move(key);

  if (command == "get") {
    c.type_ = GET;
  } else if (command == "del") {
    c.type_ = DEL;
  } else if (command == "put") {
    c.type_ = PUT;
    std::string value;
    request_stream >> value;
    if (!request_stream)
      return std::nullopt;
    c.value_ = value;
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
          if (is_from_client_) {
            HandleClientRequest();
          } else {
            HandlePeerRequest();
          }
        } else if (ec != asio::error::operation_aborted) {
          manager_->Stop(id_);
        }
      });
}

void Client::HandleClientRequest() {
  auto command = Parse(&request_);
  if (command) {
    auto r = multi_paxos_->Replicate(std::move(*command), id_);
    if (r.type_ == ResultType::kOk)
      return;
    if (r.type_ == ResultType::kRetry) {
      Write("retry");
    } else {
      CHECK(r.type_ == ResultType::kSomeoneElseLeader);
      Write("leader is ...");
    }
  } else {
    Write("bad command");
  }
}

void Client::HandlePeerRequest() {
  std::string line1;
  std::getline(std::istream(&request_), line1);
  json tcp_request = json::parse(line1);
  //asio::post(thread_pool_, [this, tcp_request] {
    std::string line = tcp_request["msg_"];
    json request = json::parse(line);
    switch((int) tcp_request["type_"]) {
    case PREPAREREQUEST: {
      auto prepare_response = multi_paxos_->Prepare(std::move(request));
      json prepare_response_json = prepare_response;
      json tcp_response = Message(PREPARERESPONSE, tcp_request["channel_id_"], 
                                  prepare_response_json.dump());
      Write(tcp_response.dump());
      break;
    }
    case ACCEPTREQUEST: {
      auto accept_response = multi_paxos_->Accept(std::move(request));
      json accept_response_json = accept_response;
      json tcp_response = Message(ACCEPTRESPONSE, tcp_request["channel_id_"], 
                                  accept_response_json.dump());
      Write(tcp_response.dump());
      break;
    }
    default: {
      auto commit_response = multi_paxos_->Commit(std::move(request));
      json commit_response_json = commit_response;
      json tcp_response = Message(COMMITRESPONSE, tcp_request["channel_id_"], 
                                  commit_response_json.dump());
      Write(tcp_response.dump());
      break;
    }
    } 
  //});
}

void Client::Write(std::string const& response) {
  std::ostream response_stream(&response_);
  response_stream << response << '\n';

  auto self(shared_from_this());
  //std::unique_lock lock(writer_mu_);
  asio::async_write(socket_, response_,
                    [this, self, response](std::error_code ec, size_t) {
                      if (ec) {
                        manager_->Stop(id_);
                      }
                      Read();
                    });
}
