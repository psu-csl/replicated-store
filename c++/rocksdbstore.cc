#include <glog/logging.h>
#include <optional>
#include <string>

#include "rocksdbstore.h"

namespace kvstore {

RocksDBStore::RocksDBStore(std::string const& path) {
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, path, &db_);
  CHECK(status.ok());
}

std::optional<std::string> RocksDBStore::Get(std::string const& key) {
  std::string value;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &value);
  if (s.ok()) {
    return value;
  }
  return std::nullopt;
}

bool RocksDBStore::Put(std::string const& key, std::string const& value) {
  rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), key, value);
  if (s.ok()) {
    return true;
  }
  return false;
}

bool RocksDBStore::Del(std::string const& key) {
  rocksdb::Status s = db_->Delete(rocksdb::WriteOptions(), key);
  if (s.ok()) {
    return true;
  }
  return false;
}

}