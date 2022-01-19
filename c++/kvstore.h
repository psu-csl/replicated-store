#ifndef KVSTORE_H_
#define KVSTORE_H_

class KVStore {
 public:
  virtual ~KVStore() = default;
  virtual std::string* get(const std::string& key) = 0;
  virtual bool put(const std::string& key, const std::string& value) = 0;
  virtual bool del(const std::string& key) = 0;
};

#endif
