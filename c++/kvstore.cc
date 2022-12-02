#include "json.h"
#include "kvstore.h"
#include "memkvstore.h"
#include "rocksdbstore.h"
#include <glog/logging.h>

namespace kvstore {

std::unique_ptr<KVStore> CreateStore(nlohmann::json const& config) {
  if (config["store"] == "rocksdb") {
    return std::make_unique<RocksDBStore>(config["db_path"]);
  } else {
    return std::make_unique<MemKVStore>();
  }
}

KVResult Execute(multipaxos::Command const& command, KVStore* store) {
  if (command.type() == multipaxos::CommandType::GET) {
    auto r = store->Get(command.key());
    if (!r)
      return KVResult{false, kNotFound};
    return KVResult{true, std::move(*r)};
  }

  if (command.type() == multipaxos::CommandType::PUT) {
    if (store->Put(command.key(), command.value()))
      return KVResult{true, kEmpty};
    return KVResult{false, kPutFailed};
  }

  CHECK(command.type() == multipaxos::CommandType::DEL);

  if (store->Del(command.key()))
    return KVResult{true, kEmpty};
  return KVResult{false, kNotFound};
}

}  // namespace kvstore
