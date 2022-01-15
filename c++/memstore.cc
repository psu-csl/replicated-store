#include "memstore.h"

std::string* MemStore::get(const std::string& key) {
  auto it = map_.find(key);
  if (it != map_.end())
    return &it->second;
  return nullptr;
}

bool MemStore::put(const std::string& key, const std::string& value) {
  map_[key] = value;
  return true;
}

bool MemStore::del(const std::string& key) {
  return map_.erase(key) != 0;
}
