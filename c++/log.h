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

using log_t = std::unordered_map<int64_t, Instance>;

bool Insert(log_t* log, Instance instance);

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

  void CommitUntil(int64_t leader_last_executed, int64_t ballot);
  void TrimUntil(int64_t leader_global_last_executed);

  bool IsExecutable(void) const {
    auto it = log_.find(last_executed_ + 1);
    return it != log_.end() && it->second.IsCommitted();
  }

  Instance const* operator[](std::size_t i) const;

 private:
  log_t log_;
  int64_t last_index_ = 0;
  int64_t last_executed_ = 0;
  int64_t global_last_executed_ = 0;
  mutable std::mutex mu_;
  std::condition_variable cv_executable_;
  std::condition_variable cv_commitable_;
};

#endif
