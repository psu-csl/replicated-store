#include <glog/logging.h>

#include "log.h"

std::tuple<client_id_t, Result> Log::Execute(KVStore* kv) {
  std::unique_lock lock(mu_);
  while (!IsExecutable())
    cv_.wait(lock);

  auto it = log_.find(last_executed_ + 1);
  CHECK(it != log_.end());
  Instance* instance = &it->second;

  CHECK_NOTNULL(kv);
  Result result = kv->Execute(instance->command_);
  instance->state_ = InstanceState::kExecuted;
  ++last_executed_;
  return {instance->client_id_, result};
}

void Log::Append(Instance instance) {
  std::scoped_lock lock(mu_);

  int64_t i = instance.index_;
  if (i <= global_last_executed_)
    return;

  if (instance.state_ == InstanceState::kExecuted)
    instance.state_ = InstanceState::kCommitted;

  auto it = log_.find(i);
  if (it == log_.end()) {
    CHECK(i > last_executed_) << "case 2 violation";
    log_[i] = std::move(instance);
    last_index_ = std::max(last_index_, i);
    return;
  }

  if (it->second.state_ == InstanceState::kCommitted ||
      it->second.state_ == InstanceState::kExecuted) {
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

Instance const* Log::operator[](std::size_t i) const {
  auto it = log_.find(i);
  return it == log_.end() ? nullptr : &it->second;
}
