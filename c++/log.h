#ifndef LOG_H_
#define LOG_H_

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <tuple>
#include <unordered_map>

#include "command.h"
#include "instance.h"
#include "kvstore.h"

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

  void Append(Instance instance);
  void Commit(int64_t index);
  std::tuple<client_id_t, Result> Execute(KVStore* kv);

  bool IsExecutable(void) const {
    auto it = log_.find(last_executed_ + 1);
    return it != log_.end() && it->second.IsCommitted();
  }

  Instance const* operator[](std::size_t i) const;

 private:
  std::unordered_map<int64_t, Instance> log_;
  int64_t last_index_ = 0;
  int64_t last_executed_ = 0;
  int64_t global_last_executed_ = 0;
  mutable std::mutex mu_;
  std::condition_variable cv_executable_;
  std::condition_variable cv_commitable_;
};

#endif
