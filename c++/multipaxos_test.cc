#include <gtest/gtest.h>

#include "json.h"
#include "log.h"
#include "multipaxos.h"

std::string MakeConfig(int64_t id) {
  return R"({ "id": )" + std::to_string(id) + R"(,
              "peers": [ "127.0.0.1:3000",
                         "127.0.0.1:3001",
                         "127.0.0.1:3002",
                         "127.0.0.1:3003",
                         "127.0.0.1:3004"]
            })";
}

TEST(MultiPaxosTest, Constructor) {
  Log log;
  MultiPaxos mp(&log, json::parse(MakeConfig(0)));

  EXPECT_EQ(kMaxNumPeers, mp.Leader());
  EXPECT_FALSE(mp.IsLeader());
  EXPECT_FALSE(mp.IsSomeoneElseLeader());
}

TEST(MultiPaxosTest, NextBallot) {
  const int kNumPeers = 5;
  for (auto id = 0; id < kNumPeers; ++id) {
    Log log;
    MultiPaxos mp(&log, json::parse(MakeConfig(id)));

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
