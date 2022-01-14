#ifndef COMMAND_H_
#define COMMAND_H_

#include <string>

enum class CommandType {kGet, kPut, kDel};

struct Command {
  CommandType type;
  std::string key;
  std::string value;
};

struct Result {
  bool ok = false;
  std::string value;
};

#endif
