#include "log.h"

bool Log::Executable(void) const {
  auto it = log_.find(last_executed_ + 1);
  return it != log_.end() && it->second.state_ == InstanceState::kCommitted;
}
