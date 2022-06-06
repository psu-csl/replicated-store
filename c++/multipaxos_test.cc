#include <gtest/gtest.h>
#include "json.h"

#include "log.h"
#include "multipaxos.h"

class MultiPaxosTest : public testing::Test {
 public:
  MultiPaxosTest()
      : config_("{ \"id\": 0 }"_json), multi_paxos_(&log_, config_) {}

 protected:
  json config_;
  Log log_;
  MultiPaxos multi_paxos_;
};

TEST_F(MultiPaxosTest, Constructor) {
  EXPECT_EQ(kMaxNumPeers, multi_paxos_.Leader());
  EXPECT_FALSE(multi_paxos_.IsLeader());
  EXPECT_FALSE(multi_paxos_.IsSomeoneElseLeader());
}
