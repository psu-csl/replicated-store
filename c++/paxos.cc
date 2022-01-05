#include "paxos.h"

bool Paxos::AgreeAndExecute(const Command& command) {
  switch (command.type) {
    case CommandType::kGet: {
      store_->get(command.key);
      break;
    }
    case CommandType::kPut: {
      store_->put(command.key, command.value);
      break;
    }
    case CommandType::kDel: {
      store_->del(command.key);
      break;
    }
  }
  return true;
}
