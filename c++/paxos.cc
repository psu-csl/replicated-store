#include <thread>

#include "paxos.h"

Paxos::Paxos(KVStore* store) : leader_(false), store_(store) {
  std::thread(&Paxos::HeartBeat, this).detach();
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

      // if we stopped being a leader, break out of this loop and go back to
      // sleep until we become a leader again
      {
        std::lock_guard lock(mu_);
        if (!leader_)
          break;
      }
    }
  }
}
