#ifndef COMMAND_H_
#define COMMAND_H_

#include <string>

enum class CommandType { kGet, kPut, kDel };

struct Command {
  CommandType type_ = CommandType::kGet;
  std::string key_;
  std::string value_;
};

bool operator==(Command const& lhs, Command const& rhs);

std::ostream& operator<<(std::ostream& os, Command const& cmd);

struct Result {
  bool ok_ = false;
  std::string const* value_ = nullptr;
};

#endif
