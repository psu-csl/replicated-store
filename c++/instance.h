#ifndef INSTANCE_H_
#define INSTANCE_H_

#include <cstdint>

#include "command.h"

enum class InstanceState { kInProgress, kCommitted, kExecuted };

using client_id_t = int64_t;

struct Instance {
  int64_t round_ = 0;
  int64_t index_ = 0;
  client_id_t client_id_ = 0;
  InstanceState state_ = InstanceState::kInProgress;
  Command command_;
};

#endif
