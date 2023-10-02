#ifndef TEST_UTIL_H
#define TEST_UTIL_H

#include "msg.h"

Instance MakeInstance(int64_t ballot, int64_t index);

Instance MakeInstance(int64_t ballot, int64_t index, InstanceState state);

Instance MakeInstance(int64_t ballot, int64_t index, CommandType type);

Instance MakeInstance(int64_t ballot,
                      int64_t index,
                      InstanceState state,
                      CommandType type);

std::string MakeConfig(int64_t id, int64_t num_peers);

#endif
