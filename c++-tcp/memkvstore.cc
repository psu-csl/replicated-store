#include <glog/logging.h>
#include <optional>
#include <string>

#include "memkvstore.h"

namespace kvstore {

std::optional<std::string> MemKVStore::Get(std::string const& key) {
  auto it = map_.find(key);
  if (it != map_.end())
    return it->second;
  return std::nullopt;
}

bool MemKVStore::Put(std::string const& key, std::string const& value) {
  map_[key] = value;
  return true;
}

bool MemKVStore::Del(std::string const& key) {
  return map_.erase(key) != 0;
}

}  // namespace kvstore
