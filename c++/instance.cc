#include <iostream>

#include "instance.h"

static char const* const states[] = {"in-progress", "committed", "executed"};

bool operator==(Instance const& lhs, Instance const& rhs) {
  return lhs.ballot_ == rhs.ballot_ && lhs.index_ == rhs.index_ &&
         lhs.client_id_ == rhs.client_id_ && lhs.state_ == rhs.state_ &&
         lhs.command_ == rhs.command_;
}

std::ostream& operator<<(std::ostream& os, Instance const& instance) {
  os << "[ballot: " << instance.ballot_ << ", index: " << instance.index_
     << ", state: " << states[static_cast<int>(instance.state_)]
     << ", command: " << instance.command_ << "]";
  return os;
}
