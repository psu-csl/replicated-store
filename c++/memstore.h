#include <map>
#include <string>

#include "kvstore.h"

class MemStore : public KVStore {
public:
  MemStore() = default;
  ~MemStore() = default;
  std::string *get(const std::string& key) override;
  bool put(const std::string& key, const std::string& value) override;
  bool del(const std::string& key) override;
private:
  std::map<std::string, std::string> map;
};
