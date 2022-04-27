#include <map>
#include <string>

#include "command.h"
#include "kvstore.h"

class MemStore : public KVStore {
 public:
  MemStore() = default;
  MemStore(MemStore const& store) = delete;
  MemStore& operator=(MemStore const& store) = delete;
  MemStore(MemStore&& store) = delete;
  MemStore& operator=(MemStore&& store) = delete;

  std::string* Get(const std::string& key) override;
  bool Put(const std::string& key, const std::string& value) override;
  bool Del(const std::string& key) override;
  Result Execute(const Command& cmd) override;

 private:
  std::map<std::string, std::string> map_;
};
