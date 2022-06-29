#include <gtest/gtest.h>
#include <chrono>
#include <thread>

#include "json.h"
#include "log.h"
#include "multipaxos.h"

using namespace std::chrono_literals;

using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerContext;
using grpc::Status;

using multipaxosrpc::HeartbeatRequest;
using multipaxosrpc::HeartbeatResponse;
using multipaxosrpc::MultiPaxosRPC;

std::string MakeConfig(int64_t id) {
  return R"({ "id": )" + std::to_string(id) + R"(,
              "peers": [ "127.0.0.1:3000",
                         "127.0.0.1:3001",
                         "127.0.0.1:3002",
                         "127.0.0.1:3003",
                         "127.0.0.1:3004"]
            })";
}

class MultiPaxosTest : public testing::Test {
 public:
  MultiPaxosTest()
      : config0_(json::parse(MakeConfig(0))),
        config1_(json::parse(MakeConfig(1))),
        config2_(json::parse(MakeConfig(2))),
        config3_(json::parse(MakeConfig(3))),
        peer0_(&log0_, config0_),
        peer1_(&log1_, config1_),
        peer2_(&log2_, config2_),
        peer3_(&log3_, config3_) {}

 protected:
  json config0_, config1_, config2_, config3_;
  Log log0_, log1_, log2_, log3_;
  MultiPaxos peer0_, peer1_, peer2_, peer3_;
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

  peer0_.Shutdown();
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

  peer0_.Shutdown();
  t0.join();
}

TEST_F(MultiPaxosTest, HeartbeatUpdatesLeaderOnFollowers) {
  std::thread t0([this] { peer0_.Start(); });
  std::thread t1([this] { peer1_.Start(); });

  auto stub0 = MultiPaxosRPC::NewStub(grpc::CreateChannel(
      config0_["peers"][0], grpc::InsecureChannelCredentials()));
  auto stub1 = MultiPaxosRPC::NewStub(grpc::CreateChannel(
      config1_["peers"][1], grpc::InsecureChannelCredentials()));

  peer0_.NextBallot();
  peer1_.NextBallot();

  auto ballot = peer2_.NextBallot();
  {
    ClientContext context;
    HeartbeatRequest request;
    HeartbeatResponse response;

    request.set_ballot(ballot);
    stub0->Heartbeat(&context, request, &response);
  }
  {
    ClientContext context;
    HeartbeatRequest request;
    HeartbeatResponse response;

    request.set_ballot(ballot);
    stub1->Heartbeat(&context, request, &response);
  }

  EXPECT_FALSE(peer0_.IsLeader());
  EXPECT_FALSE(peer1_.IsLeader());
  EXPECT_EQ(2, peer0_.Leader());
  EXPECT_EQ(2, peer1_.Leader());

  ballot = peer3_.NextBallot();
  {
    ClientContext context;
    HeartbeatRequest request;
    HeartbeatResponse response;

    request.set_ballot(ballot);
    stub0->Heartbeat(&context, request, &response);
  }
  {
    ClientContext context;
    HeartbeatRequest request;
    HeartbeatResponse response;

    request.set_ballot(ballot);
    stub1->Heartbeat(&context, request, &response);
  }

  EXPECT_FALSE(peer0_.IsLeader());
  EXPECT_FALSE(peer1_.IsLeader());
  EXPECT_EQ(3, peer0_.Leader());
  EXPECT_EQ(3, peer1_.Leader());

  peer0_.Shutdown();
  peer1_.Shutdown();
  t0.join();
  t1.join();
}
