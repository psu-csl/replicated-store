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

TEST_F(MultiPaxosTest, NextBallot) {
  for (int id = 0; id < kMaxNumPeers; ++id) {
    json config = json::parse("{ \"id\": " + std::to_string(id) + " }");
    MultiPaxos mp(&log_, config);
    int64_t ballot = id;

    ballot += kRoundIncrement;
    EXPECT_EQ(ballot, mp.NextBallot());
    ballot += kRoundIncrement;
    EXPECT_EQ(ballot, mp.NextBallot());

    EXPECT_TRUE(mp.IsLeader());
    EXPECT_FALSE(mp.IsSomeoneElseLeader());
    EXPECT_EQ(id, mp.Leader());
  }
}
