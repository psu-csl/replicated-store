#include <iostream>

#include "command.h"

bool operator==(Command const& lhs, Command const& rhs) {
  return lhs.type_ == rhs.type_ && lhs.key_ == rhs.key_ &&
         lhs.value_ == rhs.value_;
}

static char const* const types[] = {"get", "put", "del"};

std::ostream& operator<<(std::ostream& os, Command const& cmd) {
  os << "{" << types[static_cast<int>(cmd.type_)] << ":" << cmd.key_ << "|"
     << cmd.value_ << "}";
  return os;
}
