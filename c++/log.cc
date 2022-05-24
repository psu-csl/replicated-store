#include <glog/logging.h>

#include "log.h"

void Log::Append(Instance instance) {
  std::scoped_lock lock(mu_);

  int64_t i = instance.index_;
  if (i <= global_last_executed_)
    return;

  if (instance.IsExecuted())
    instance.SetCommitted();

  auto it = log_.find(i);
  if (it == log_.end()) {
    CHECK(i > last_executed_) << "case 2 violation";
    log_[i] = std::move(instance);
    last_index_ = std::max(last_index_, i);
    cv_commitable_.notify_all();
    return;
  }

  if (it->second.IsCommitted() || it->second.IsExecuted()) {
    CHECK(it->second.command_ == instance.command_) << "case 3 violation";
    return;
  }

  if (it->second.ballot_ < instance.ballot_) {
    log_[i] = std::move(instance);
    return;
  }

  if (it->second.ballot_ == instance.ballot_)
    CHECK(it->second.command_ == instance.command_) << "case 4 violation";
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
  std::unique_lock lock(mu_);
  for (auto i = last_executed_ + 1; i <= leader_last_executed; ++i) {
    auto it = log_.find(i);
    if (it == log_.end())
      break;
    if (it->second.ballot_ == ballot)
      it->second.SetCommitted();
  }
  if (IsExecutable())
    cv_executable_.notify_one();
}

Instance const* Log::operator[](std::size_t i) const {
  auto it = log_.find(i);
  return it == log_.end() ? nullptr : &it->second;
}
