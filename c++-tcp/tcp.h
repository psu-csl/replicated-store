#ifndef TCP_H
#define TCP_H

#include <atomic>
#include <boost/asio.hpp>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "atomicops.h"
#include "blockingconcurrentqueue.h"
#include "msg.h"
#include "readerwriterqueue.h"

using asio::ip::tcp;
using moodycamel::BlockingReaderWriterQueue;
using moodycamel::BlockingConcurrentQueue;

struct ChannelMap {
  mutable std::mutex mu_;
  std::unordered_map<
      int64_t, BlockingConcurrentQueue<std::string>&> map_;
};

class TcpLink : public std::enable_shared_from_this<TcpLink> {
 public:
  TcpLink(std::string const address, 
          asio::io_context* io_context);

  bool Connect();

  void SendAwaitResponse(MessageType type,
                         int64_t channel_id,
                         BlockingConcurrentQueue<std::string>& channel,
                         std::string const& msg);

  void HandleOutgoingRequests(tcp::socket& socket, 
  	  BlockingConcurrentQueue<std::string>& request_channel);
  void HandleIncomingResponses(tcp::socket& socket, 
                               ChannelMap& channels);
  void Stop(); 

 private:
  std::string address_;
  asio::io_context* io_context_;
  BlockingConcurrentQueue<std::string> request_channel_;
  tcp::socket socket_;
  std::thread incoming_thread_;
  std::thread outgoing_thread_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::atomic<bool> is_connected_;
  ChannelMap channels;
};

#endif
