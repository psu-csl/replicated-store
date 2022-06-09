#ifndef MULTI_PAXOS_H_
#define MULTI_PAXOS_H_

#include <atomic>
#include <cstdint>
#include <mutex>

#include "json_fwd.h"
#include "kvstore.h"
#include "log.h"

static const int64_t kIdBits = 0xff;
static const int64_t kRoundIncrement = kIdBits + 1;
static const int64_t kMaxNumPeers = 0xf;

using nlohmann::json;

class MultiPaxos {
 public:
  MultiPaxos(Log* log, json const& config);
  MultiPaxos(Log const& log) = delete;
  MultiPaxos& operator=(MultiPaxos const& log) = delete;
  MultiPaxos(MultiPaxos&& log) = delete;
  MultiPaxos& operator=(MultiPaxos&& log) = delete;

  int64_t id(void) const { return id_; }

  int64_t NextBallot(void) {
    std::scoped_lock lock(mu_);
    ballot_ += kRoundIncrement;
    ballot_ = (ballot_ & ~kIdBits) | id_;
    return ballot_;
  }

  int64_t Leader(void) const {
    std::scoped_lock lock(mu_);
    return ballot_ & kIdBits;
  }

  bool IsLeader(void) const {
    std::scoped_lock lock(mu_);
    return (ballot_ & kIdBits) == id_;
  }

  bool IsSomeoneElseLeader() const {
    auto id = Leader();
    return id != id_ && id < kMaxNumPeers;
  }

 private:
  int64_t id_;
  int64_t ballot_;
  mutable std::mutex mu_;
};

#endif
