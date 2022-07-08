#include <gtest/gtest.h>
#include <chrono>
#include <thread>

#include "json.h"
#include "log.h"
#include "multipaxos.h"

using namespace std::chrono;

using nlohmann::json;

using grpc::ClientContext;

using multipaxos::HeartbeatRequest;
using multipaxos::HeartbeatResponse;
using multipaxos::MultiPaxosRPC;

std::string MakeConfig(int64_t id) {
  return R"({ "id": )" + std::to_string(id) + R"(,
              "threadpool_size": 8,
              "heartbeat_delta": 10,
              "heartbeat_interval": 300,
              "peers": [ "127.0.0.1:3000",
                         "127.0.0.1:3001",
                         "127.0.0.1:3002"]
            })";
}

class MultiPaxosTest : public testing::Test {
 public:
  MultiPaxosTest()
      : config0_(json::parse(MakeConfig(0))),
        config1_(json::parse(MakeConfig(1))),
        config2_(json::parse(MakeConfig(2))),
        peer0_(&log0_, config0_),
        peer1_(&log1_, config1_),
        peer2_(&log2_, config2_) {}

 protected:
  json config0_, config1_, config2_;
  Log log0_, log1_, log2_;
  MultiPaxos peer0_, peer1_, peer2_;
};

TEST_F(MultiPaxosTest, Constructor) {
  EXPECT_EQ(kMaxNumPeers, peer0_.Leader());
  EXPECT_FALSE(peer0_.IsLeader());
  EXPECT_FALSE(peer0_.IsSomeoneElseLeader());
}

TEST_F(MultiPaxosTest, NextBallot) {
  int peer2 = 2;
  int ballot = peer2;

  ballot += kRoundIncrement;
  EXPECT_EQ(ballot, peer2_.NextBallot());
  ballot += kRoundIncrement;
  EXPECT_EQ(ballot, peer2_.NextBallot());

  EXPECT_TRUE(peer2_.IsLeader());
  EXPECT_FALSE(peer2_.IsSomeoneElseLeader());
  EXPECT_EQ(peer2, peer2_.Leader());
}

TEST_F(MultiPaxosTest, HeartbeatIgnoreStaleRPC) {
  std::thread t0([this] { peer0_.Start(); });

  auto stub0 = MultiPaxosRPC::NewStub(grpc::CreateChannel(
      config0_["peers"][0], grpc::InsecureChannelCredentials()));

  peer0_.NextBallot();
  peer0_.NextBallot();

  ClientContext context0;
  HeartbeatRequest request0;
  HeartbeatResponse response0;

  request0.set_ballot(peer1_.NextBallot());

  stub0->Heartbeat(&context0, request0, &response0);

  EXPECT_TRUE(peer0_.IsLeader());

  peer0_.Stop();
  t0.join();
}

TEST_F(MultiPaxosTest, HeartbeatChangesLeaderToFollower) {
  std::thread t0([this] { peer0_.Start(); });

  auto stub0 = MultiPaxosRPC::NewStub(grpc::CreateChannel(
      config0_["peers"][0], grpc::InsecureChannelCredentials()));

  ClientContext context0;
  HeartbeatRequest request0;
  HeartbeatResponse response0;

  peer0_.NextBallot();
  request0.set_ballot(peer1_.NextBallot());
  stub0->Heartbeat(&context0, request0, &response0);

  EXPECT_FALSE(peer0_.IsLeader());
  EXPECT_EQ(1, peer0_.Leader());

  peer0_.Stop();
  t0.join();
}
