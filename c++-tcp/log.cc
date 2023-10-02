#include <glog/logging.h>

#include "log.h"

using InstanceState::COMMITTED;
using InstanceState::EXECUTED;

bool IsCommitted(Instance const& instance) {
  return instance.state_ == InstanceState::COMMITTED;
}
bool IsExecuted(Instance const& instance) {
  return instance.state_ == InstanceState::EXECUTED;
}
bool IsInProgress(Instance const& instance) {
  return instance.state_ == InstanceState::INPROGRESS;
}

bool operator==(Command const& a, Command const& b) {
  return a.type_ == b.type_ && a.key_ == b.key_ && a.value_ == b.value_;
}

bool operator==(Instance const& a, Instance const& b) {
  return a.ballot_ == b.ballot_ && a.index_ == b.index_ &&
         a.client_id_ == b.client_id_ && a.state_ == b.state_ &&
         a.command_ == b.command_;
}

bool Insert(std::unordered_map<int64_t, Instance>* log, Instance instance) {
  auto i = instance.index_;
  auto it = log->find(i);
  if (it == log->end()) {
    (*log)[i] = std::move(instance);
    return true;
  }
  if (IsCommitted(it->second) || IsExecuted(it->second)) {
    CHECK(it->second.command_ == instance.command_) << "Insert case2";
    return false;
  }
  if (instance.ballot_ > it->second.ballot_) {
    (*log)[i] = std::move(instance);
    return false;
  }
  if (instance.ballot_ == it->second.ballot_)
    CHECK(it->second.command_ == instance.command_) << "Insert case3";
  return false;
}

void Log::Append(Instance instance) {
  std::scoped_lock lock(mu_);

  int64_t i = instance.index_;
  if (i <= global_last_executed_)
    return;

  if (Insert(&log_, std::move(instance))) {
    last_index_ = std::max(last_index_, i);
    cv_committable_.notify_all();
  }
}

void Log::Commit(int64_t index) {
  CHECK(index > 0) << "invalid index";

  std::unique_lock lock(mu_);
  auto it = log_.find(index);
  while (it == log_.end()) {
    cv_committable_.wait(lock);
    it = log_.find(index);
  }

  if (IsInProgress(it->second))
    it->second.state_ = COMMITTED;

  if (IsExecutable())
    cv_executable_.notify_one();
}

std::optional<std::tuple<int64_t, kvstore::KVResult>> Log::Execute() {
  std::unique_lock lock(mu_);
  while (running_ && !IsExecutable())
    cv_executable_.wait(lock);

  if (!running_)
    return std::nullopt;

  auto it = log_.find(last_executed_ + 1);
  CHECK(it != log_.end());
  Instance* instance = &it->second;
  kvstore::KVResult result =
      kvstore::Execute(instance->command_, kv_store_.get());
  instance->state_ = EXECUTED;
  ++last_executed_;
  return {{instance->client_id_, std::move(result)}};
}

void Log::CommitUntil(int64_t leader_last_executed, int64_t ballot) {
  CHECK(leader_last_executed >= 0) << "invalid leader_last_executed";
  CHECK(ballot >= 0) << "invalid ballot";

  std::unique_lock lock(mu_);
  for (auto i = last_executed_ + 1; i <= leader_last_executed; ++i) {
    auto it = log_.find(i);
    if (it == log_.end())
      break;
    CHECK(ballot >= it->second.ballot_) << "CommitUntil case 2";
    if (it->second.ballot_ == ballot)
      it->second.state_ = COMMITTED;
  }
  if (IsExecutable())
    cv_executable_.notify_one();
}

void Log::TrimUntil(int64_t leader_global_last_executed) {
  std::scoped_lock lock(mu_);
  while (global_last_executed_ < leader_global_last_executed) {
    ++global_last_executed_;
    auto it = log_.find(global_last_executed_);
    CHECK(it != log_.end() && IsExecuted(it->second)) << "TrimUntil case 1";
    log_.erase(it);
  }
}

std::vector<Instance> Log::Instances() const {
  std::scoped_lock lock(mu_);
  std::vector<Instance> instances;
  for (auto i = global_last_executed_ + 1; i <= last_index_; ++i)
    if (auto it = log_.find(i); it != log_.end())
      instances.push_back(it->second);
  return instances;
}

Instance const* Log::at(std::size_t i) const {
  auto it = log_.find(i);
  return it == log_.end() ? nullptr : &it->second;
}

std::unordered_map<int64_t, Instance> Log::GetLog() {
  std::unordered_map<int64_t, Instance> local_log(log_);
  return local_log;
}
