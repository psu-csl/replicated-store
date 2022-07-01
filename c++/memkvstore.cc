#include <glog/logging.h>
#include <string>

#include "memkvstore.h"

using multipaxos::Command;

std::string* MemKVStore::Get(const std::string& key) {
  auto it = map_.find(key);
  if (it != map_.end())
    return &it->second;
  return nullptr;
}

bool MemKVStore::Put(const std::string& key, const std::string& value) {
  map_[key] = value;
  return true;
}

bool MemKVStore::Del(const std::string& key) {
  return map_.erase(key) != 0;
}

Result MemKVStore::Execute(const Command& cmd) {
  if (cmd.type() == multipaxos::CommandType::GET) {
    auto r = Get(cmd.key());
    if (!r)
      return Result{false, &kKeyNotFound};
    return Result{true, r};
  }

  if (cmd.type() == multipaxos::CommandType::PUT) {
    Put(cmd.key(), cmd.value());
    return Result{true, &kEmpty};
  }

  CHECK(cmd.type() == multipaxos::CommandType::DEL);

  if (Del(cmd.key()))
    return Result{true, &kEmpty};
  return Result{false, &kKeyNotFound};
}
