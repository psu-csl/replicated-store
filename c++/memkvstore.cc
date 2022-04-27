#include <glog/logging.h>
#include <string>

#include "command.h"
#include "memkvstore.h"

static std::string kKeyNotFound = "key not found";
static std::string kEmpty = "";

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
  if (cmd.type_ == CommandType::kGet) {
    auto r = Get(cmd.key_);
    if (!r)
      return Result{false, &kKeyNotFound};
    return Result{true, r};
  }

  if (cmd.type_ == CommandType::kPut) {
    Put(cmd.key_, cmd.value_);
    return Result{true, &kEmpty};
  }

  CHECK(cmd.type_ == CommandType::kDel);

  if (Del(cmd.key_))
    return Result{true, &kEmpty};
  return Result{false, &kKeyNotFound};
}
