#ifndef KVSTORE_H_
#define KVSTORE_H_

#include <string>

#include "command.h"

class KVStore {
 public:
  virtual ~KVStore() = default;
  virtual std::string* Get(const std::string& key) = 0;
  virtual bool Put(const std::string& key, const std::string& value) = 0;
  virtual bool Del(const std::string& key) = 0;
  virtual Result Execute(const Command& cmd) = 0;
};

#endif
