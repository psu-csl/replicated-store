#include <glog/logging.h>

#include "log.h"

std::tuple<client_id_t, Result> Log::Execute(KVStore* kv) {
  std::unique_lock lock(mu_);
  while (!IsExecutable())
    cv_.wait(lock);

  auto it = log_.find(last_executed_ + 1);
  CHECK(it != log_.end());
  Instance const* instance = &it->second;

  CHECK_NOTNULL(kv);
  Result result = kv->Execute(instance->command_);
  ++last_executed_;
  return {instance->client_id_, result};
}
