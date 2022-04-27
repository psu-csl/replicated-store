#include "paxos.h"

#include <glog/logging.h>
#include <chrono>
#include <thread>
#include "json.h"

Paxos::Paxos(KVStore* store, const json& config)
    : id_(config["id"]),
      leader_(config["leader"]),
      heartbeat_pause_(config["heartbeat_pause"]),
      store_(store),
      rpc_service_(this),
      tp_(config["threadpool_size"]) {
  // establish client rpc channels to peers
  std::string me = config["peers"][id_];
  for (const std::string peer : config["peers"]) {
    if (peer != me) {
      rpc_peers_.emplace_back(
          this, grpc::CreateChannel(peer, grpc::InsecureChannelCredentials()));
    }
  }

  // start the rpc service
  ServerBuilder builder;
  builder.AddListeningPort(me, grpc::InsecureServerCredentials());
  builder.RegisterService(&rpc_service_);
  rpc_server_ = builder.BuildAndStart();
  CHECK(rpc_server_);
  DLOG(INFO) << "listening for rpc requests at " << me;

  // start the heartbeat thread
  std::thread(&Paxos::HeartBeat, this).detach();
}

Result Paxos::AgreeAndExecute(Command cmd) {
  return store_->Execute(cmd);
}

void Paxos::HeartBeat(void) {
  DLOG(INFO) << "peer " << id_ << " heartbeat thread starting...";
  for (;;) {
    // wait until we become a leader
    {
      std::unique_lock lock(mu_);
      while (!leader_)
        cv_.wait(lock);
    }
    // we are a leader; start sending heartbeats until we stop being a leader
    for (;;) {
      uint32_t num_replies = 0;     // total number of rpc replies
      std::vector<int> ok_replies;  // values of replies from successful rpcs
      std::mutex mu;
      std::condition_variable cv;
      for (auto& peer : rpc_peers_) {
        asio::post(tp_, [this, &num_replies, &ok_replies, &mu, &peer, &cv] {
          int rc = peer.Heartbeat(min_last_executed());
          {
            std::lock_guard lock(mu);
            ++num_replies;
            if (rc != -1)
              ok_replies.push_back(rc);
          }
          cv.notify_one();
        });
      }
      {
        std::unique_lock lock(mu);
        while (ok_replies.size() < rpc_peers_.size() / 2 &&
               num_replies != rpc_peers_.size()) {
          cv.wait(lock);
        }
      }
      // we need to set min_last_executed only if we have ok from the majority
      {
        std::lock_guard lock(mu);
        if (ok_replies.size() >= rpc_peers_.size() / 2)
          set_min_last_executed(
              *min_element(begin(ok_replies), end(ok_replies)));
      }
      // pause and then check if we stopped being a leader; go to sleep if so
      std::this_thread::sleep_for(heartbeat_pause_);
      {
        std::lock_guard lock(mu_);
        if (!leader_)
          break;
      }
    }
  }
}
