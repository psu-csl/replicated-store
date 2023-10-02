#include <glog/logging.h>

#include "test_util.h"

Instance MakeInstance(int64_t ballot, int64_t index) {
  Instance i(ballot, index, 0, INPROGRESS, Command());
  return i;
}

Instance MakeInstance(int64_t ballot, int64_t index, InstanceState state) {
  auto i = MakeInstance(ballot, index);
  i.state_ = state;
  return i;
}

Instance MakeInstance(int64_t ballot, int64_t index, CommandType type) {
  auto i = MakeInstance(ballot, index);
  i.command_.type_ = type;
  return i;
}

Instance MakeInstance(int64_t ballot,
                      int64_t index,
                      InstanceState state,
                      CommandType type) {
  auto i = MakeInstance(ballot, index);
  i.state_ = state;
  i.command_.type_ = type;
  return i;
}

std::string MakeConfig(int64_t id, int64_t num_peers) {
  CHECK(id < num_peers);
  auto r = R"({ "id": )" + std::to_string(id) + R"(,
              "threadpool_size": 8,
              "commit_interval": 300,
              "store": "mem",
              "peers": [)";

  for (auto i = 0; i < num_peers; ++i) {
    r += R"("127.0.0.1:1)" + std::to_string(i) + R"(000")";
    if (i + 1 < num_peers)
      r += R"(,)";
  }
  r += R"(]})";

  return r;
}
