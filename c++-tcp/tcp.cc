#include <asio.hpp>
#include <glog/logging.h>
#include <thread>

#include "json.h"
#include "tcp.h"

using asio::ip::tcp;
using asio::ip::address;
using nlohmann::json;

TcpLink::TcpLink(std::string const address,
  	             ChannelMap& channels,
  	             asio::io_context* io_context)
    : address_(std::move(address)),
      io_context_(io_context),
      socket_(asio::make_strand(*io_context)),
      is_connected_(false) {
  Connect();
  incoming_thread_ = std::thread([this, &channels]() {
      HandleIncomingResponses(socket_, channels);});
  outgoing_thread_ = std::thread([this]() {
      HandleOutgoingRequests(socket_, request_channel_);});
}

bool TcpLink::Connect() {
  auto pos = address_.find(":") + 1;
  auto ip = address_.substr(0, pos - 1);
  int port = std::stoi(address_.substr(pos));
  tcp::endpoint endpoint(address::from_string(ip), port);
  std::error_code ec;
  socket_.connect(endpoint, ec);
  if (!ec) {
    std::unique_lock lock(mu_);
    is_connected_ = true;
    cv_.notify_one();
    return true;
  }
  return false;
}

void TcpLink::SendAwaitResponse(MessageType type, 
	                              int64_t channel_id, 
	                              std::string const& msg) {
  Message request(type, channel_id, msg);
  json j = request;
  request_channel_.enqueue(j.dump() + "\n");
}

void TcpLink::HandleOutgoingRequests(tcp::socket& socket, 
  	BlockingConcurrentQueue<std::string>& request_channel) {
  for (;;) {
  	std::string request;
  	request_channel.wait_dequeue(request);
    if (request == "EOF")
      break;
  	asio::error_code error;
    if (is_connected_ || (!is_connected_ && Connect())) {
  	  socket.write_some(asio::buffer(request, request.size()), error);
      if (error) {
    		break;
      }
    }
  }
}
  
void TcpLink::HandleIncomingResponses(tcp::socket& socket,
	                                    ChannelMap& channels) {
  asio::error_code error;
  asio::streambuf response_buf;
  for (;;) {
    while (!is_connected_) {
      std::unique_lock lock(mu_);
      cv_.wait(lock);
    }

  	asio::read_until(socket, response_buf, '\n', error);
  	if (error)
  		break;
    std::istream response_stream(&response_buf);
    std::string response_str;
    std::getline(response_stream, response_str);
  	json response = json::parse(response_str);
  	int64_t channel_id = response["channel_id_"];
  	{
  	  std::unique_lock lock(channels.mu_);
  	  auto it = channels.map_.find(channel_id);
  	  if (it != channels.map_.end()) {
        std::string msg = response["msg_"];
  	  	it->second.enqueue(msg);
  	  }
  	}
  }
}

void TcpLink::Stop() {
  { 
    std::unique_lock lock(mu_);
    is_connected_ = true;
    cv_.notify_one();
  if (is_connected_)
    socket_.close();
  }
  request_channel_.enqueue("EOF");
  incoming_thread_.join();
  outgoing_thread_.join();
}
