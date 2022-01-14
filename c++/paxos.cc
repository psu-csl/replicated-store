#include "paxos.h"

Result Paxos::AgreeAndExecute(Command command) {
  Result r;
  switch (command.type) {
    case CommandType::kGet: {
      auto v = store_->get(command.key);
      if (v != nullptr) {
        r.ok = true;
        r.value = *v;
      }
      break;
    }
    case CommandType::kPut: {
      r.ok = store_->put(command.key, command.value);
      break;
    }
    case CommandType::kDel: {
      r.ok = store_->del(command.key);
      break;
    }
  }
  return r;
}
