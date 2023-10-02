#include "json.h"
#include "kvstore.h"
#include "memkvstore.h"
#include "rocksdbstore.h"
#include <glog/logging.h>

namespace kvstore {

std::unique_ptr<KVStore> CreateStore(nlohmann::json const& config) {
  if (config["store"] == "rocksdb") {
    return std::make_unique<RocksDBStore>(config["db_path"]);
  } else if (config["store"] == "mem") {
    return std::make_unique<MemKVStore>();
  } else {
      CHECK(false);
  }
}

KVResult Execute(Command const& command, KVStore* store) {
  if (command.type_ == CommandType::GET) {
    auto r = store->Get(command.key_);
    if (!r)
      return KVResult{false, kNotFound};
    return KVResult{true, std::move(*r)};
  }

  if (command.type_ == CommandType::PUT) {
    if (store->Put(command.key_, command.value_))
      return KVResult{true, kEmpty};
    return KVResult{false, kPutFailed};
  }

  CHECK(command.type_ == CommandType::DEL);

  if (store->Del(command.key_))
    return KVResult{true, kEmpty};
  return KVResult{false, kNotFound};
}

}  // namespace kvstore
