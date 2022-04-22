#ifndef INSTANCE_H_
#define INSTANCE_H_

#include <cstdint>

#include "command.h"

enum class InstanceState { kInProgress, kCommitted, kExecuted };

struct Instance {
  int64_t round_;
  int64_t index_;
  int64_t client_id_;
  InstanceState state_;
  Command command_;
};

#endif
