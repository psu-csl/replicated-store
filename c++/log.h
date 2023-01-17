#ifndef LOG_H_
#define LOG_H_

#include <glog/logging.h>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "kvstore.h"

bool Insert(std::unordered_map<int64_t, multipaxos::Instance>* log,
            multipaxos::Instance instance);
bool IsCommitted(multipaxos::Instance const& instance);
bool IsExecuted(multipaxos::Instance const& instance);
bool IsInProgress(multipaxos::Instance const& instance);

namespace multipaxos {
bool operator==(multipaxos::Command const& a, multipaxos::Command const& b);
bool operator==(multipaxos::Instance const& a, multipaxos::Instance const& b);
}  // namespace multipaxos

class Log {
 public:
  explicit Log(std::unique_ptr<kvstore::KVStore> kv_store)
      : kv_store_(std::move(kv_store)) {}
  Log(Log const& log) = delete;
  Log& operator=(Log const& log) = delete;
  Log(Log&& log) = delete;
  Log& operator=(Log&& log) = delete;

  int64_t LastExecuted() const {
    std::scoped_lock lock(mu_);
    return last_executed_;
  }

  int64_t GlobalLastExecuted() const {
    std::scoped_lock lock(mu_);
    return global_last_executed_;
  }

  int64_t AdvanceLastIndex() {
    std::scoped_lock lock(mu_);
    return ++last_index_;
  }

  void SetLastIndex(int64_t last_index) {
    std::scoped_lock lock(mu_);
    last_index_ = std::max(last_index_, last_index);
  }

  int64_t LastIndex() const {
    std::scoped_lock lock(mu_);
    return last_index_;
  }

  void Stop() {
    std::scoped_lock lock(mu_);
    running_ = false;
    cv_executable_.notify_one();
  }

  void Append(multipaxos::Instance instance);
  void Commit(int64_t index);
  std::optional<std::tuple<int64_t, kvstore::KVResult>> Execute();

  void CommitUntil(int64_t leader_last_executed, int64_t ballot);
  void TrimUntil(int64_t leader_global_last_executed);

  std::vector<multipaxos::Instance> Instances() const;

  bool IsExecutable() const {
    auto it = log_.find(last_executed_ + 1);
    return it != log_.end() && IsCommitted(it->second);
  }

  multipaxos::Instance const* at(std::size_t i) const;
  std::unordered_map<int64_t, multipaxos::Instance> GetLog();

 private:
  bool running_ = true;
  std::unique_ptr<kvstore::KVStore> kv_store_;
  std::unordered_map<int64_t, multipaxos::Instance> log_;
  int64_t last_index_ = 0;
  int64_t last_executed_ = 0;
  int64_t global_last_executed_ = 0;
  mutable std::mutex mu_;
  std::condition_variable cv_executable_;
  std::condition_variable cv_committable_;
};

#endif
