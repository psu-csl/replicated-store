#include "test_util.h"

using multipaxos::Command;
using multipaxos::CommandType;
using multipaxos::Instance;
using multipaxos::InstanceState;

using multipaxos::InstanceState::INPROGRESS;

Instance MakeInstance(int64_t ballot, int64_t index) {
  Instance i;
  i.set_ballot(ballot);
  i.set_index(index);
  i.set_state(INPROGRESS);
  *i.mutable_command() = Command();
  return i;
}

Instance MakeInstance(int64_t ballot, int64_t index, CommandType type) {
  Instance i;
  i.set_ballot(ballot);
  i.set_index(index);
  i.set_state(INPROGRESS);
  Command c;
  c.set_type(type);
  *i.mutable_command() = c;
  return i;
}

Instance MakeInstance(int64_t ballot,
                      int64_t index,
                      InstanceState state,
                      CommandType type) {
  Instance i;
  i.set_ballot(ballot);
  i.set_index(index);
  i.set_state(state);
  Command c;
  c.set_type(type);
  *i.mutable_command() = c;
  return i;
}
