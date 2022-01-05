#include "memstore.h"

std::string* MemStore::get(const std::string& key) {
  auto it = map.find(key);
  if (it != map.end())
    return &it->second;
  return nullptr;
}

bool MemStore::put(const std::string& key, const std::string& value) {
  map[key] = value;
  return true;
}

bool MemStore::del(const std::string& key) {
  return map.erase(key) != 0;
}
