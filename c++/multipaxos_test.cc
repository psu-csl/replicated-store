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

  bool OneLeader() const {
    if (peer0_.IsLeaderTest())
      return !peer1_.IsLeaderTest() && !peer2_.IsLeaderTest() &&
             peer1_.LeaderTest() == 0 && peer2_.LeaderTest() == 0;
    if (peer1_.IsLeaderTest())
      return !peer0_.IsLeaderTest() && !peer2_.IsLeaderTest() &&
             peer0_.LeaderTest() == 1 && peer2_.LeaderTest() == 1;
    if (peer2_.IsLeaderTest())
      return !peer0_.IsLeaderTest() && !peer1_.IsLeaderTest() &&
             peer0_.LeaderTest() == 2 && peer1_.LeaderTest() == 2;
    return false;
  }

 protected:
  json config0_, config1_, config2_;
  Log log0_, log1_, log2_;
  MultiPaxos peer0_, peer1_, peer2_;
};

TEST_F(MultiPaxosTest, Constructor) {
  EXPECT_EQ(kMaxNumPeers, peer0_.LeaderTest());
  EXPECT_FALSE(peer0_.IsLeaderTest());
  EXPECT_FALSE(peer0_.IsSomeoneElseLeader());
}

TEST_F(MultiPaxosTest, NextBallot) {
  int peer2 = 2;
  int ballot = peer2;

  ballot += kRoundIncrement;
  EXPECT_EQ(ballot, peer2_.NextBallot());
  ballot += kRoundIncrement;
  EXPECT_EQ(ballot, peer2_.NextBallot());

  EXPECT_TRUE(peer2_.IsLeaderTest());
  EXPECT_FALSE(peer2_.IsSomeoneElseLeader());
  EXPECT_EQ(peer2, peer2_.LeaderTest());
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

  EXPECT_TRUE(peer0_.IsLeaderTest());

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

  EXPECT_FALSE(peer0_.IsLeaderTest());
  EXPECT_EQ(1, peer0_.LeaderTest());

  peer0_.Stop();
  t0.join();
}

TEST_F(MultiPaxosTest, OneLeaderElected) {
  std::thread t0([this] { peer0_.Start(); });
  std::thread t1([this] { peer1_.Start(); });
  std::thread t2([this] { peer2_.Start(); });

  auto heartbeat_3x =
      milliseconds(3 * static_cast<int>(config0_["heartbeat_interval"]));

  std::this_thread::sleep_for(heartbeat_3x);

  EXPECT_TRUE(OneLeader());

  peer0_.Stop();
  peer1_.Stop();
  peer2_.Stop();
  t0.join();
  t1.join();
  t2.join();
}
