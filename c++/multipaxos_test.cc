#include <gtest/gtest.h>
#include <chrono>
#include <thread>

#include "json.h"
#include "log.h"
#include "memkvstore.h"
#include "multipaxos.h"
#include "test_util.h"

using namespace std::chrono;

using nlohmann::json;

using grpc::ClientContext;

using multipaxos::HeartbeatRequest;
using multipaxos::HeartbeatResponse;
using multipaxos::MultiPaxosRPC;
using multipaxos::ResponseType::OK;
using multipaxos::ResponseType::REJECT;

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

int64_t Leader(MultiPaxos const& peer) {
  auto [ballot, is_ready] = peer.Ballot();
  return Leader(ballot);
}

bool IsLeader(MultiPaxos const& peer) {
  auto [ballot, is_ready] = peer.Ballot();
  return IsLeader(ballot, peer.Id());
}

bool IsSomeoneElseLeader(MultiPaxos const& peer) {
  return !IsLeader(peer) && Leader(peer) < kMaxNumPeers;
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
    if (IsLeader(peer0_))
      return !IsLeader(peer1_) && !IsLeader(peer2_) && Leader(peer1_) == 0 &&
             Leader(peer2_) == 0;
    if (IsLeader(peer1_))
      return !IsLeader(peer0_) && !IsLeader(peer2_) && Leader(peer0_) == 1 &&
             Leader(peer2_) == 1;
    if (IsLeader(peer2_))
      return !IsLeader(peer0_) && !IsLeader(peer1_) && Leader(peer0_) == 2 &&
             Leader(peer1_) == 2;
    return false;
  }

 protected:
  json config0_, config1_, config2_;
  Log log0_, log1_, log2_;
  MultiPaxos peer0_, peer1_, peer2_;
  MemKVStore store_;
};

TEST_F(MultiPaxosTest, Constructor) {
  EXPECT_EQ(kMaxNumPeers, Leader(peer0_));
  EXPECT_FALSE(IsLeader(peer0_));
  EXPECT_FALSE(IsSomeoneElseLeader(peer0_));
}

TEST_F(MultiPaxosTest, NextBallot) {
  int peer2 = 2;
  int ballot = peer2;

  ballot += kRoundIncrement;
  EXPECT_EQ(ballot, peer2_.NextBallot());
  ballot += kRoundIncrement;
  EXPECT_EQ(ballot, peer2_.NextBallot());

  EXPECT_TRUE(IsLeader(peer2_));
  EXPECT_FALSE(IsSomeoneElseLeader(peer2_));
  EXPECT_EQ(peer2, Leader(peer2_));
}

TEST_F(MultiPaxosTest, HeartbeatIgnoreStaleRPC) {
  std::thread t([this] { peer0_.StartRPCServer(); });

  auto stub = MultiPaxosRPC::NewStub(grpc::CreateChannel(
      config0_["peers"][0], grpc::InsecureChannelCredentials()));

  peer0_.NextBallot();
  peer0_.NextBallot();

  ClientContext context;
  HeartbeatRequest request;
  HeartbeatResponse response;

  request.set_ballot(peer1_.NextBallot());

  stub->Heartbeat(&context, request, &response);

  EXPECT_TRUE(IsLeader(peer0_));

  peer0_.StopRPCServer();
  t.join();
}

TEST_F(MultiPaxosTest, HeartbeatChangesLeaderToFollower) {
  std::thread t([this] { peer0_.StartRPCServer(); });

  auto stub = MultiPaxosRPC::NewStub(grpc::CreateChannel(
      config0_["peers"][0], grpc::InsecureChannelCredentials()));

  ClientContext context;
  HeartbeatRequest request;
  HeartbeatResponse response;

  peer0_.NextBallot();
  request.set_ballot(peer1_.NextBallot());
  stub->Heartbeat(&context, request, &response);

  EXPECT_FALSE(IsLeader(peer0_));
  EXPECT_EQ(1, Leader(peer0_));

  peer0_.StopRPCServer();
  t.join();
}

TEST_F(MultiPaxosTest, HeartbeatCommitsAndTrims) {
  std::thread t([this] { peer0_.StartRPCServer(); });

  auto ballot = peer0_.NextBallot();

  auto index1 = log0_.AdvanceLastIndex();
  log0_.Append(MakeInstance(ballot, index1));

  auto index2 = log0_.AdvanceLastIndex();
  log0_.Append(MakeInstance(ballot, index2));

  auto index3 = log0_.AdvanceLastIndex();
  log0_.Append(MakeInstance(ballot, index3));

  auto stub = MultiPaxosRPC::NewStub(grpc::CreateChannel(
      config0_["peers"][0], grpc::InsecureChannelCredentials()));

  {
    ClientContext context;
    HeartbeatRequest request;
    HeartbeatResponse response;
    request.set_ballot(ballot);
    request.set_last_executed(index2);
    request.set_global_last_executed(0);

    stub->Heartbeat(&context, request, &response);
  }

  EXPECT_TRUE(IsCommitted(*log0_[index1]));
  EXPECT_TRUE(IsCommitted(*log0_[index2]));
  EXPECT_TRUE(IsInProgress(*log0_[index3]));

  log0_.Execute(&store_);
  log0_.Execute(&store_);

  {
    ClientContext context;
    HeartbeatRequest request;
    HeartbeatResponse response;
    request.set_ballot(ballot);
    request.set_last_executed(index2);
    request.set_global_last_executed(index2);

    stub->Heartbeat(&context, request, &response);
  }

  EXPECT_EQ(nullptr, log0_[index1]);
  EXPECT_EQ(nullptr, log0_[index2]);
  EXPECT_TRUE(IsInProgress(*log0_[index3]));

  peer0_.StopRPCServer();
  t.join();
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
