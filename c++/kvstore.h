#ifndef KVSTORE_H_
#define KVSTORE_H_

#include <string>

#include "multipaxos.pb.h"

struct KVResult {
  bool ok_ = false;
  std::string const* value_ = nullptr;
};

class KVStore {
 public:
  virtual ~KVStore() = default;
  virtual std::string* Get(const std::string& key) = 0;
  virtual bool Put(const std::string& key, const std::string& value) = 0;
  virtual bool Del(const std::string& key) = 0;
  virtual KVResult Execute(const multipaxos::Command& cmd) = 0;
};

#endif
