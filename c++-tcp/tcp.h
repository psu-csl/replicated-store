#ifndef TCP_H
#define TCP_H

#include <atomic>
#include <boost/asio.hpp>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "atomicops.h"
#include "msg.h"
#include "readerwriterqueue.h"

using asio::ip::tcp;
using moodycamel::BlockingReaderWriterQueue;

struct ChannelMap {
  mutable std::mutex mu_;
  std::unordered_map<
      int64_t, BlockingReaderWriterQueue<std::string>&> map_;
};

class TcpLink {
 public:
  TcpLink(std::string const address, 
  	      ChannelMap& channels, 
  	      asio::io_context* io_context);

  bool Connect();

  void SendAwaitResponse(MessageType type,
  	                     int64_t channel_id, 
  	                     std::string const& msg);

  void HandleOutgoingRequests(tcp::socket& socket, 
  	  BlockingReaderWriterQueue<std::string>& request_channel);
  void HandleIncomingResponses(tcp::socket& socket, 
                               ChannelMap& channels);
  void Stop(); 

 private:
  std::string address_;
  asio::io_context* io_context_;
  BlockingReaderWriterQueue<std::string> request_channel_;
  tcp::socket socket_;
  std::thread incoming_thread_;
  std::thread outgoing_thread_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::atomic<bool> is_connected_;
};

#endif
