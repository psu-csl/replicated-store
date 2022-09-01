#ifndef KVSTORE_H_
#define KVSTORE_H_

#include <optional>
#include <string>

#include "multipaxos.pb.h"

namespace kvstore {

static std::string const kKeyNotFound = "key not found";
static std::string const kEmpty = "";

struct KVResult {
  bool ok_ = false;
  std::string value_;
};

class KVStore {
 public:
  virtual ~KVStore() = default;
  virtual std::optional<std::string> Get(std::string const& key) = 0;
  virtual bool Put(std::string const& key, std::string const& value) = 0;
  virtual bool Del(std::string const& key) = 0;
};

KVResult Execute(multipaxos::Command const& command, KVStore* store);

}  // namespace kvstore

#endif
