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

TEST(MultiPaxosTest, HeartbeatIgnoreStaleRPC) {
  auto id = 0;
  auto config = json::parse(MakeConfig(id));
  auto peer = config["peers"][id];
  Log log;
  MultiPaxos mp(&log, config);

  std::thread t([&mp] {
    mp.Start();
    mp.NextBallot();
  });

  auto stub = MultiPaxosRPC::NewStub(
      grpc::CreateChannel(peer, grpc::InsecureChannelCredentials()));

  ClientContext context;
  HeartbeatRequest request;
  HeartbeatResponse response;

  request.set_ballot(0);
  request.set_last_executed(1);
  request.set_global_last_executed(1);

  stub->Heartbeat(&context, request, &response);

  EXPECT_EQ(0, response.last_executed());

  mp.Shutdown();
  t.join();
}
