#ifndef LOG_H_
#define LOG_H_

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "instance.h"

class Log {
 public:
  Log() = default;
  Log(Log const& log) = delete;
  Log& operator=(Log const& log) = delete;
  Log(Log&& log) = delete;
  Log& operator=(Log&& log) = delete;

  int64_t LastExecuted(void) const {
    std::scoped_lock lock(mu_);
    return last_executed_;
  }

  int64_t GlobalLastExecuted(void) const {
    std::scoped_lock lock(mu_);
    return global_last_executed_;
  }

  int64_t AdvanceLastIndex(void) {
    std::scoped_lock lock(mu_);
    return ++last_index_;
  }

 private:
  bool Executable(void) const {
    auto it = log_.find(last_executed_ + 1);
    return it != log_.end() && it->second.state_ == InstanceState::kCommitted;
  }

  std::unordered_map<int64_t, Instance> log_;
  int64_t last_index_ = 0;
  int64_t last_executed_ = 0;
  int64_t global_last_executed_ = 0;
  mutable std::mutex mu_;
  std::condition_variable cv_;
};

#endif
