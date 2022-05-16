#include "command.h"

bool operator==(Command const& lhs, Command const& rhs) {
  return lhs.type_ == rhs.type_ && lhs.key_ == rhs.key_ &&
         lhs.value_ == rhs.value_;
}
