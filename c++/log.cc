#include <glog/logging.h>

#include "log.h"

using multipaxos::Instance;
using multipaxos::InstanceState::COMMITTED;
using multipaxos::InstanceState::EXECUTED;

bool IsCommitted(multipaxos::Instance const& instance) {
  return instance.state() == multipaxos::InstanceState::COMMITTED;
}
bool IsExecuted(multipaxos::Instance const& instance) {
  return instance.state() == multipaxos::InstanceState::EXECUTED;
}
bool IsInProgress(multipaxos::Instance const& instance) {
  return instance.state() == multipaxos::InstanceState::INPROGRESS;
}

namespace multipaxos {
bool operator==(multipaxos::Command const& a, multipaxos::Command const& b) {
  return a.type() == b.type() && a.key() == b.key() && a.value() == b.value();
}

bool operator==(multipaxos::Instance const& a, multipaxos::Instance const& b) {
  return a.ballot() == b.ballot() && a.index() == b.index() &&
         a.client_id() == b.client_id() && a.state() == b.state() &&
         a.command() == b.command();
}
}  // namespace multipaxos

bool Insert(log_map_t* log, Instance instance) {
  auto i = instance.index();
  auto it = log->find(i);
  if (it == log->end()) {
    (*log)[i] = std::move(instance);
    return true;
  }
  if (IsCommitted(it->second) || IsExecuted(it->second)) {
    CHECK(it->second.command() == instance.command()) << "Insert case2";
    return false;
  }
  if (instance.ballot() > it->second.ballot()) {
    (*log)[i] = std::move(instance);
    return false;
  }
  if (instance.ballot() == it->second.ballot())
    CHECK(it->second.command() == instance.command()) << "Insert case3";
  return false;
}

void Log::Append(Instance instance) {
  std::scoped_lock lock(mu_);

  int64_t i = instance.index();
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
    it->second.set_state(COMMITTED);

  if (IsExecutable())
    cv_executable_.notify_one();
}

std::optional<log_result_t> Log::Execute() {
  std::unique_lock lock(mu_);
  while (running_ && !IsExecutable())
    cv_executable_.wait(lock);

  if (!running_)
    return std::nullopt;

  auto it = log_.find(last_executed_ + 1);
  CHECK(it != log_.end());
  Instance* instance = &it->second;
  kvstore::KVResult result = kvstore::Execute(instance->command(), kv_store_);
  instance->set_state(EXECUTED);
  ++last_executed_;
  return {{instance->client_id(), std::move(result)}};
}

void Log::CommitUntil(int64_t leader_last_executed, int64_t ballot) {
  CHECK(leader_last_executed >= 0) << "invalid leader_last_executed";
  CHECK(ballot >= 0) << "invalid ballot";

  std::unique_lock lock(mu_);
  for (auto i = last_executed_ + 1; i <= leader_last_executed; ++i) {
    auto it = log_.find(i);
    if (it == log_.end())
      break;
    CHECK(ballot >= it->second.ballot()) << "CommitUntil case 2";
    if (it->second.ballot() == ballot)
      it->second.set_state(COMMITTED);
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

log_vector_t Log::Instances() const {
  std::scoped_lock lock(mu_);
  log_vector_t instances;
  for (auto i = global_last_executed_ + 1; i <= last_index_; ++i)
    if (auto it = log_.find(i); it != log_.end())
      instances.push_back(it->second);
  return instances;
}

Instance const* Log::operator[](std::size_t i) const {
  auto it = log_.find(i);
  return it == log_.end() ? nullptr : &it->second;
}
