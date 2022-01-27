#include "paxos.h"

#include <glog/logging.h>
#include <thread>
#include "json.h"

Paxos::Paxos(const json& config, KVStore* store)
    : id_(config["me"]), leader_(false), store_(store) {
  std::string me = config["peers"][id_];

  for (const auto& peer : config["peers"]) {
    if (peer != me) {
      rpc_clients_.emplace_back(
          grpc::CreateChannel(peer, grpc::InsecureChannelCredentials()));
    }
  }

  std::thread(&Paxos::HeartBeat, this).detach();

  LOG(INFO) << "Peer " << id_ << " listening for RPC calls at " << me;
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
  LOG(INFO) << "Peer " << id_ << " heartbeat thread starting...";
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
      // send RPCs
      // compute minLastExecuted from the replies

      // check if we stopped being a leader, and if so, break out of this loop
      // to the outer loop, and go back to sleep until we become a leader again.
      {
        std::lock_guard lock(mu_);
        if (!leader_)
          break;
      }
    }
  }
}
