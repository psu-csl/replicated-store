#include <map>
#include <string>

#include "kvstore.h"

namespace kvstore {

static std::string const kKeyNotFound = "key not found";
static std::string const kEmpty = "";

class MemKVStore : public KVStore {
 public:
  MemKVStore() = default;
  MemKVStore(MemKVStore const& store) = delete;
  MemKVStore& operator=(MemKVStore const& store) = delete;
  MemKVStore(MemKVStore&& store) = delete;
  MemKVStore& operator=(MemKVStore&& store) = delete;

  std::optional<std::string> Get(std::string const& key) override;
  bool Put(std::string const& key, std::string const& value) override;
  bool Del(std::string const& key) override;
  KVResult Execute(multipaxos::Command const& cmd) override;

 private:
  std::map<std::string, std::string> map_;
};

}  // namespace kvstore
