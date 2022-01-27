#include "paxos.h"

#include <glog/logging.h>
#include <chrono>
#include <thread>
#include "json.h"

Paxos::Paxos(const json& config, KVStore* store)
    : id_(config["me"]),
      heartbeat_pause_(config["heartbeat_pause"]),
      store_(store),
      tp_(std::thread::hardware_concurrency()),
      rpc_server_(this) {
  // start the heartbeat thread
  std::thread(&Paxos::HeartBeat, this).detach();

  // establish client RPC channels to peers
  std::string me = config["peers"][id_];
  for (const auto& peer : config["peers"]) {
    if (peer != me) {
      rpc_peers_.emplace_back(
          grpc::CreateChannel(peer, grpc::InsecureChannelCredentials()));
    }
  }

  // start the RPC service
  LOG(INFO) << "peer " << id_ << " listening for RPC calls at " << me;
  builder_.AddListeningPort(me, grpc::InsecureServerCredentials());
  builder_.RegisterService(&rpc_server_);
  builder_.BuildAndStart();
}

Result Paxos::AgreeAndExecute(Command cmd) {
  Result r;
  switch (cmd.type) {
    case CommandType::kGet: {
      auto v = store_->get(cmd.key);
      if (v != nullptr) {
        r.ok = true;
        r.value = *v;
      }
      break;
    }
    case CommandType::kPut: {
      r.ok = store_->put(cmd.key, cmd.value);
      break;
    }
    case CommandType::kDel: {
      r.ok = store_->del(cmd.key);
      break;
    }
  }
  return r;
}

void Paxos::HeartBeat(void) {
  LOG(INFO) << "peer " << id_ << " heartbeat thread starting...";
  for (;;) {
    // wait until we become a leader
    {
      std::unique_lock lock(mu_);
      while (!leader_)
        cv_.wait(lock);
    }
    // now we are a leader; start sending heartbeats until we stop being a
    // leader.
    for (;;) {
      uint32_t num_replies = 0;     // total number of RPC replies
      std::vector<int> ok_replies;  // values of replies from non-failed RPCs
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
        while (ok_replies.size() < rpc_peers_.size() / 2 ||
               num_replies != rpc_peers_.size())
          cv.wait(lock);
      }
      // at this point, we either have majority of ok replies or we received all
      // the replies most of which are failed RPCs. so we need to set
      // min_last_executed only if we have ok replies from the majority.
      {
        std::lock_guard lock(mu);
        if (ok_replies.size() >= rpc_peers_.size() / 2)
          set_min_last_executed(
              *min_element(begin(ok_replies), end(ok_replies)));
      }

      // pause and then check if we stopped being a leader; if so, break out of
      // this loop, and go back to sleep until we become a leader again.
      std::this_thread::sleep_for(heartbeat_pause_);
      {
        std::lock_guard lock(mu_);
        if (!leader_)
          break;
      }
    }
  }
}
