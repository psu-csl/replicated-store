#include <map>
#include <string>

#include "command.h"
#include "kvstore.h"

static std::string kKeyNotFound = "key not found";
static std::string kEmpty = "";

class MemKVStore : public KVStore {
 public:
  MemKVStore() = default;
  MemKVStore(MemKVStore const& store) = delete;
  MemKVStore& operator=(MemKVStore const& store) = delete;
  MemKVStore(MemKVStore&& store) = delete;
  MemKVStore& operator=(MemKVStore&& store) = delete;

  std::string* Get(const std::string& key) override;
  bool Put(const std::string& key, const std::string& value) override;
  bool Del(const std::string& key) override;
  Result Execute(const Command& cmd) override;

 private:
  std::map<std::string, std::string> map_;
};
