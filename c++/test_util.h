#ifndef TEST_UTIL_H
#define TEST_UTIL_H

#include "multipaxos.pb.h"

multipaxos::Instance MakeInstance(int64_t ballot, int64_t index);

multipaxos::Instance MakeInstance(int64_t ballot,
                                  int64_t index,
                                  multipaxos::InstanceState state);

multipaxos::Instance MakeInstance(int64_t ballot,
                                  int64_t index,
                                  multipaxos::CommandType type);

multipaxos::Instance MakeInstance(int64_t ballot,
                                  int64_t index,
                                  multipaxos::InstanceState state,
                                  multipaxos::CommandType type);

#endif
