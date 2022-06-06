#ifndef MULTI_PAXOS_H_
#define MULTI_PAXOS_H_

#include <atomic>
#include <cstdint>
#include <mutex>

#include "json_fwd.h"
#include "kvstore.h"
#include "log.h"

static const int64_t kIdBits = 0xff;
static const int64_t kMaxNumPeers = 0xf;

using nlohmann::json;

class MultiPaxos {
 public:
  MultiPaxos(Log* log, json const& config);
  MultiPaxos(Log const& log) = delete;
  MultiPaxos& operator=(MultiPaxos const& log) = delete;
  MultiPaxos(MultiPaxos&& log) = delete;
  MultiPaxos& operator=(MultiPaxos&& log) = delete;

  int64_t NextBallot(void) {
    std::scoped_lock lock(mu_);
    ballot_ += kIdBits;
    ballot_ = (ballot_ & ~kIdBits) | id_;
    return ballot_;
  }

  int64_t Leader(void) const { return ballot_ & kIdBits; }

  bool IsLeader(void) const { return Leader() == id_; }

  bool IsSomeoneElseLeader() const {
    auto id = Leader();
    return id != id_ && id < kMaxNumPeers;
  }

 private:
  int64_t id_;
  std::atomic<int64_t> ballot_;
  std::mutex mu_;
};

#endif
