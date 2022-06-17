#include <glog/logging.h>

#include "log.h"

bool Insert(log_t* log, Instance instance) {
  auto i = instance.index_;
  auto it = log->find(i);
  if (it == log->end()) {
    (*log)[i] = std::move(instance);
    return true;
  }
  if (it->second.IsCommitted() || it->second.IsExecuted()) {
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
    cv_commitable_.notify_all();
  }
}

void Log::Commit(int64_t index) {
  CHECK(index > 0) << "invalid index";

  std::unique_lock lock(mu_);
  auto it = log_.find(index);
  while (it == log_.end()) {
    cv_commitable_.wait(lock);
    it = log_.find(index);
  }

  if (it->second.IsInProgress())
    it->second.SetCommitted();

  if (IsExecutable())
    cv_executable_.notify_one();
}

std::tuple<client_id_t, Result> Log::Execute(KVStore* kv) {
  std::unique_lock lock(mu_);
  while (!IsExecutable())
    cv_executable_.wait(lock);

  auto it = log_.find(last_executed_ + 1);
  CHECK(it != log_.end());
  Instance* instance = &it->second;

  CHECK_NOTNULL(kv);
  Result result = kv->Execute(instance->command_);
  instance->SetExecuted();
  ++last_executed_;
  return {instance->client_id_, result};
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
      it->second.SetCommitted();
  }
  if (IsExecutable())
    cv_executable_.notify_one();
}

void Log::TrimUntil(int64_t leader_global_last_executed) {
  CHECK(leader_global_last_executed >= global_last_executed_)
      << "invalid leader_global_last_executed";

  std::scoped_lock lock(mu_);
  while (global_last_executed_ < leader_global_last_executed) {
    ++global_last_executed_;
    auto it = log_.find(global_last_executed_);
    CHECK(it != log_.end() && it->second.IsExecuted()) << "TrimUntil case 1";
    log_.erase(it);
  }
}

std::vector<Instance> Log::InstancesForPrepare() const {
  std::scoped_lock lock(mu_);
  std::vector<Instance> instances;
  for (auto i = global_last_executed_ + 1; i <= last_index_; ++i)
    if (auto it = log_.find(i); it != log_.end())
      instances.push_back(it->second);
  return instances;
}

Instance const* Log::operator[](std::size_t i) const {
  auto it = log_.find(i);
  return it == log_.end() ? nullptr : &it->second;
}
