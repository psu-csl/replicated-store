#include <iostream>

#include "instance.h"

static char const* const states[] = {"in-progress", "committed", "executed"};

std::ostream& operator<<(std::ostream& os, Instance const& instance) {
  os << "[ballot: " << instance.ballot_ << ", index: " << instance.index_
     << ", state: " << states[static_cast<int>(instance.state_)]
     << ", command: " << instance.command_ << "]";
  return os;
}
