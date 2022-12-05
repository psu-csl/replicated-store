#include <string>

#include "kvstore.h"
#include "rocksdb/db.h"

namespace kvstore {

class RocksDBStore : public KVStore {
 public:
  RocksDBStore(std::string const& path);
  RocksDBStore(RocksDBStore const& store) = delete;
  RocksDBStore& operator=(RocksDBStore const& store) = delete;
  RocksDBStore(RocksDBStore&& store) = delete;
  RocksDBStore& operator=(RocksDBStore&& store) = delete;

  ~RocksDBStore() {
      db_->Close();
  }

  std::optional<std::string> Get(std::string const& key) override;
  bool Put(std::string const& key, std::string const& value) override;
  bool Del(std::string const& key) override;

 private:
  rocksdb::DB* db_;
};

}
