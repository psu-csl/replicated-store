#include <gtest/gtest.h>
#include <chrono>
#include <memory>
#include <thread>

#include "json.h"
#include "log.h"
#include "memkvstore.h"
#include "multipaxos.h"
#include "test_util.h"

using namespace std::chrono;

using nlohmann::json;

using grpc::ClientContext;

using multipaxos::Command;
using multipaxos::Instance;
using multipaxos::MultiPaxosRPC;

using multipaxos::AcceptRequest;
using multipaxos::AcceptResponse;

using multipaxos::HeartbeatRequest;
using multipaxos::HeartbeatResponse;

using multipaxos::PrepareRequest;
using multipaxos::PrepareResponse;

using multipaxos::ResponseType::OK;
using multipaxos::ResponseType::REJECT;

static const int kNumPeers = 3;

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

HeartbeatResponse SendHeartbeat(MultiPaxosRPC::Stub* stub,
                                int64_t ballot,
                                int64_t last_executed,
                                int64_t global_last_executed) {
  ClientContext context;
  HeartbeatRequest request;
  HeartbeatResponse response;

  request.set_ballot(ballot);
  request.set_last_executed(last_executed);
  request.set_global_last_executed(global_last_executed);
  stub->Heartbeat(&context, request, &response);
  return response;
}

PrepareResponse SendPrepare(MultiPaxosRPC::Stub* stub, int64_t ballot) {
  ClientContext context;
  PrepareRequest request;
  PrepareResponse response;

  request.set_ballot(ballot);
  stub->Prepare(&context, request, &response);
  return response;
}

AcceptResponse SendAccept(MultiPaxosRPC::Stub* stub, Instance const& instance) {
  ClientContext context;
  AcceptRequest request;
  AcceptResponse response;

  *request.mutable_instance() = instance;
  stub->Accept(&context, request, &response);
  return response;
}

std::unique_ptr<MultiPaxosRPC::Stub> MakeStub(std::string const& target) {
  return MultiPaxosRPC::NewStub(
      grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));
}

std::string MakeConfig(int64_t id) {
  CHECK(id < kNumPeers);
  auto r = R"({ "id": )" + std::to_string(id) + R"(,
              "threadpool_size": 8,
              "heartbeat_delta": 10,
              "heartbeat_interval": 300,
              "peers": [)";

  for (auto i = 0; i < kNumPeers; ++i) {
    r += R"("127.0.0.1:300)" + std::to_string(i) + R"(")";
    if (i + 1 < kNumPeers)
      r += R"(,)";
  }
  r += R"(]})";

  return r;
}

class MultiPaxosTest : public testing::Test {
 public:
  MultiPaxosTest() {
    for (auto id = 0; id < kNumPeers; ++id) {
      auto config = json::parse(MakeConfig(id));
      auto log = std::make_unique<Log>();
      auto peer = std::make_unique<MultiPaxos>(log.get(), config);

      configs_.push_back(config);
      logs_.push_back(std::move(log));
      peers_.push_back(std::move(peer));
    }
  }

  MultiPaxos* OneLeader() const {
    auto leader = Leader(*peers_[0]);
    auto num_leaders = 0;
    for (auto const& p : peers_)
      if (IsLeader(*p)) {
        ++num_leaders;
        if (num_leaders > 1 || p->Id() != leader)
          return nullptr;
      } else if (Leader(*p) != leader) {
        return nullptr;
      }
    return peers_[leader].get();
  }

 protected:
  std::vector<json> configs_;
  std::vector<std::unique_ptr<Log>> logs_;
  std::vector<std::unique_ptr<MultiPaxos>> peers_;
  MemKVStore store_;
};

TEST_F(MultiPaxosTest, Constructor) {
  EXPECT_EQ(kMaxNumPeers, Leader(*peers_[0]));
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_FALSE(IsSomeoneElseLeader(*peers_[0]));
}

TEST_F(MultiPaxosTest, NextBallot) {
  int peer2 = 2;
  int ballot = peer2;

  ballot += kRoundIncrement;
  EXPECT_EQ(ballot, peers_[2]->NextBallot());
  ballot += kRoundIncrement;
  EXPECT_EQ(ballot, peers_[2]->NextBallot());

  EXPECT_TRUE(IsLeader(*peers_[2]));
  EXPECT_FALSE(IsSomeoneElseLeader(*peers_[2]));
  EXPECT_EQ(peer2, Leader(*peers_[2]));
}

TEST_F(MultiPaxosTest, RequestsWithLowerBallotIgnored) {
  std::thread t([this] { peers_[0]->StartRPCServer(); });
  auto stub = MakeStub(configs_[0]["peers"][0]);

  peers_[0]->NextBallot();
  peers_[0]->NextBallot();
  auto stale_ballot = peers_[1]->NextBallot();

  auto r1 = SendHeartbeat(stub.get(), stale_ballot, 0, 0);
  EXPECT_EQ(REJECT, r1.type());
  EXPECT_TRUE(IsLeader(*peers_[0]));

  auto r2 = SendPrepare(stub.get(), stale_ballot);
  EXPECT_EQ(REJECT, r2.type());
  EXPECT_TRUE(IsLeader(*peers_[0]));

  auto index = logs_[0]->AdvanceLastIndex();
  auto instance = MakeInstance(stale_ballot, index);
  auto r3 = SendAccept(stub.get(), instance);
  EXPECT_EQ(REJECT, r3.type());
  EXPECT_TRUE(IsLeader(*peers_[0]));
  EXPECT_EQ(nullptr, (*logs_[0])[index]);

  peers_[0]->StopRPCServer();
  t.join();
}

TEST_F(MultiPaxosTest, RequestsWithHigherBallotChangeLeaderToFollower) {
  std::thread t([this] { peers_[0]->StartRPCServer(); });
  auto stub = MakeStub(configs_[0]["peers"][0]);

  peers_[0]->NextBallot();
  EXPECT_TRUE(IsLeader(*peers_[0]));
  auto r1 = SendHeartbeat(stub.get(), peers_[1]->NextBallot(), 0, 0);
  EXPECT_EQ(OK, r1.type());
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(1, Leader(*peers_[0]));

  peers_[0]->NextBallot();
  EXPECT_TRUE(IsLeader(*peers_[0]));
  auto r2 = SendPrepare(stub.get(), peers_[1]->NextBallot());
  EXPECT_EQ(OK, r2.type());
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(1, Leader(*peers_[0]));

  peers_[0]->NextBallot();
  EXPECT_TRUE(IsLeader(*peers_[0]));
  auto index = logs_[0]->AdvanceLastIndex();
  auto instance = MakeInstance(peers_[1]->NextBallot(), index);
  auto r3 = SendAccept(stub.get(), instance);
  EXPECT_EQ(OK, r3.type());
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(1, Leader(*peers_[0]));

  peers_[0]->StopRPCServer();
  t.join();
}

TEST_F(MultiPaxosTest, HeartbeatCommitsAndTrims) {
  std::thread t([this] { peers_[0]->StartRPCServer(); });
  auto stub = MakeStub(configs_[0]["peers"][0]);

  auto ballot = peers_[0]->NextBallot();
  auto index1 = logs_[0]->AdvanceLastIndex();
  logs_[0]->Append(MakeInstance(ballot, index1));
  auto index2 = logs_[0]->AdvanceLastIndex();
  logs_[0]->Append(MakeInstance(ballot, index2));
  auto index3 = logs_[0]->AdvanceLastIndex();
  logs_[0]->Append(MakeInstance(ballot, index3));

  auto r1 = SendHeartbeat(stub.get(), ballot, index2, 0);
  EXPECT_EQ(OK, r1.type());
  EXPECT_EQ(0, r1.last_executed());
  EXPECT_TRUE(IsCommitted(*(*logs_[0])[index1]));
  EXPECT_TRUE(IsCommitted(*(*logs_[0])[index2]));
  EXPECT_TRUE(IsInProgress(*(*logs_[0])[index3]));

  logs_[0]->Execute(&store_);
  logs_[0]->Execute(&store_);

  auto r2 = SendHeartbeat(stub.get(), ballot, index2, index2);
  EXPECT_EQ(OK, r2.type());
  EXPECT_EQ(index2, r2.last_executed());
  EXPECT_EQ(nullptr, (*logs_[0])[index1]);
  EXPECT_EQ(nullptr, (*logs_[0])[index2]);
  EXPECT_TRUE(IsInProgress(*(*logs_[0])[index3]));

  peers_[0]->StopRPCServer();
  t.join();
}

TEST_F(MultiPaxosTest, PrepareRespondsWithCorrectInstances) {
  std::thread t([this] { peers_[0]->StartRPCServer(); });
  auto stub = MakeStub(configs_[0]["peers"][0]);

  auto ballot = peers_[0]->NextBallot();

  auto index1 = logs_[0]->AdvanceLastIndex();
  auto instance1 = MakeInstance(ballot, index1);
  logs_[0]->Append(instance1);

  auto index2 = logs_[0]->AdvanceLastIndex();
  auto instance2 = MakeInstance(ballot, index2);
  logs_[0]->Append(instance2);

  auto index3 = logs_[0]->AdvanceLastIndex();
  auto instance3 = MakeInstance(ballot, index3);
  logs_[0]->Append(instance3);

  auto r1 = SendPrepare(stub.get(), ballot);
  EXPECT_EQ(OK, r1.type());
  EXPECT_EQ(3, r1.instances_size());
  EXPECT_EQ(instance1, r1.instances(0));
  EXPECT_EQ(instance2, r1.instances(1));
  EXPECT_EQ(instance3, r1.instances(2));

  auto r2 = SendHeartbeat(stub.get(), ballot, index2, 0);
  EXPECT_EQ(OK, r2.type());

  logs_[0]->Execute(&store_);
  logs_[0]->Execute(&store_);

  auto r3 = SendPrepare(stub.get(), ballot);
  EXPECT_EQ(OK, r3.type());
  EXPECT_EQ(3, r3.instances_size());
  EXPECT_TRUE(IsExecuted(r3.instances(0)));
  EXPECT_TRUE(IsExecuted(r3.instances(1)));
  EXPECT_EQ(instance3, r3.instances(2));

  auto r4 = SendHeartbeat(stub.get(), ballot, index2, 2);
  EXPECT_EQ(OK, r4.type());

  auto r5 = SendPrepare(stub.get(), ballot);
  EXPECT_EQ(OK, r5.type());
  EXPECT_EQ(1, r5.instances_size());
  EXPECT_EQ(instance3, r5.instances(0));

  peers_[0]->StopRPCServer();
  t.join();
}

TEST_F(MultiPaxosTest, AcceptAppendsToLog) {
  std::thread t([this] { peers_[0]->StartRPCServer(); });
  auto stub = MakeStub(configs_[0]["peers"][0]);

  auto ballot = peers_[0]->NextBallot();
  auto index1 = logs_[0]->AdvanceLastIndex();
  auto instance1 = MakeInstance(ballot, index1);
  auto index2 = logs_[0]->AdvanceLastIndex();
  auto instance2 = MakeInstance(ballot, index2);

  auto r1 = SendAccept(stub.get(), instance1);
  EXPECT_EQ(OK, r1.type());
  EXPECT_EQ(instance1, *(*logs_[0])[index1]);
  EXPECT_EQ(nullptr, (*logs_[0])[index2]);

  auto r2 = SendAccept(stub.get(), instance2);
  EXPECT_EQ(OK, r2.type());
  EXPECT_EQ(instance1, *(*logs_[0])[index1]);
  EXPECT_EQ(instance2, *(*logs_[0])[index2]);

  peers_[0]->StopRPCServer();
  t.join();
}

TEST_F(MultiPaxosTest, HeartbeatResponseWithHighBallotChangesLeaderToFollower) {
  std::thread t0([this] { peers_[0]->StartRPCServer(); });
  std::thread t1([this] { peers_[1]->StartRPCServer(); });
  std::thread t2([this] { peers_[2]->StartRPCServer(); });
  auto stub1 = MakeStub(configs_[0]["peers"][1]);

  auto peer0_ballot = peers_[0]->NextBallot();
  peers_[1]->NextBallot();
  auto peer2_ballot = peers_[2]->NextBallot();

  auto r = SendHeartbeat(stub1.get(), peer2_ballot, 0, 0);
  EXPECT_EQ(OK, r.type());
  EXPECT_FALSE(IsLeader(*peers_[1]));
  EXPECT_EQ(2, Leader(*peers_[1]));

  EXPECT_TRUE(IsLeader(*peers_[0]));
  peers_[0]->SendHeartbeats(peer0_ballot, 0);
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(2, Leader(*peers_[0]));

  peers_[0]->StopRPCServer();
  peers_[1]->StopRPCServer();
  peers_[2]->StopRPCServer();
  t0.join();
  t1.join();
  t2.join();
}

TEST_F(MultiPaxosTest, PrepareResponseWithHighBallotChangesLeaderToFollower) {
  std::thread t0([this] { peers_[0]->StartRPCServer(); });
  std::thread t1([this] { peers_[1]->StartRPCServer(); });
  std::thread t2([this] { peers_[2]->StartRPCServer(); });
  auto stub1 = MakeStub(configs_[0]["peers"][1]);

  auto peer0_ballot = peers_[0]->NextBallot();
  peers_[1]->NextBallot();
  auto peer2_ballot = peers_[2]->NextBallot();

  auto r = SendHeartbeat(stub1.get(), peer2_ballot, 0, 0);
  EXPECT_EQ(OK, r.type());
  EXPECT_FALSE(IsLeader(*peers_[1]));
  EXPECT_EQ(2, Leader(*peers_[1]));

  EXPECT_TRUE(IsLeader(*peers_[0]));
  peers_[0]->SendPrepares(peer0_ballot);
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(2, Leader(*peers_[0]));

  peers_[0]->StopRPCServer();
  peers_[1]->StopRPCServer();
  peers_[2]->StopRPCServer();
  t0.join();
  t1.join();
  t2.join();
}

TEST_F(MultiPaxosTest, AcceptResponseWithHighBallotChangesLeaderToFollower) {
  std::thread t0([this] { peers_[0]->StartRPCServer(); });
  std::thread t1([this] { peers_[1]->StartRPCServer(); });
  std::thread t2([this] { peers_[2]->StartRPCServer(); });
  auto stub1 = MakeStub(configs_[0]["peers"][1]);

  auto peer0_ballot = peers_[0]->NextBallot();
  peers_[1]->NextBallot();
  auto peer2_ballot = peers_[2]->NextBallot();

  auto r = SendHeartbeat(stub1.get(), peer2_ballot, 0, 0);
  EXPECT_EQ(OK, r.type());
  EXPECT_FALSE(IsLeader(*peers_[1]));
  EXPECT_EQ(2, Leader(*peers_[1]));

  EXPECT_TRUE(IsLeader(*peers_[0]));
  peers_[0]->SendAccepts(peer0_ballot, 1, Command(), 0);
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(2, Leader(*peers_[0]));

  peers_[0]->StopRPCServer();
  peers_[1]->StopRPCServer();
  peers_[2]->StopRPCServer();
  t0.join();
  t1.join();
  t2.join();
}

TEST_F(MultiPaxosTest, OneLeaderElected) {
  std::thread t0([this] { peers_[0]->Start(); });
  std::thread t1([this] { peers_[1]->Start(); });
  std::thread t2([this] { peers_[2]->Start(); });

  int heartbeat = configs_[0]["heartbeat_interval"];
  auto heartbeat_3x = 3 * milliseconds(heartbeat);

  std::this_thread::sleep_for(heartbeat_3x);

  EXPECT_NE(nullptr, OneLeader());

  peers_[0]->Stop();
  peers_[1]->Stop();
  peers_[2]->Stop();
  t0.join();
  t1.join();
  t2.join();
}
