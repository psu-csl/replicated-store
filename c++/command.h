#ifndef COMMAND_H_
#define COMMAND_H_

#include <string>

enum class CommandType { kGet, kPut, kDel };

struct Command {
  CommandType type_;
  std::string key_;
  std::string value_;
};

#endif
