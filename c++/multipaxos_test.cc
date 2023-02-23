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

using multipaxos::AcceptRequest;
using multipaxos::AcceptResponse;
using multipaxos::Command;
using multipaxos::CommitRequest;
using multipaxos::CommitResponse;
using multipaxos::Instance;
using multipaxos::MultiPaxosRPC;
using multipaxos::PrepareRequest;
using multipaxos::PrepareResponse;
using multipaxos::CommandType::DEL;
using multipaxos::CommandType::GET;
using multipaxos::CommandType::PUT;
using multipaxos::InstanceState::COMMITTED;
using multipaxos::InstanceState::EXECUTED;
using multipaxos::InstanceState::INPROGRESS;
using multipaxos::ResponseType::OK;
using multipaxos::ResponseType::REJECT;

static const int kNumPeers = 3;

int64_t Leader(MultiPaxos const& peer) {
  return ExtractLeaderId(peer.Ballot());
}

bool IsLeader(MultiPaxos const& peer) {
  return IsLeader(peer.Ballot(), peer.Id());
}

bool IsSomeoneElseLeader(MultiPaxos const& peer) {
  return !IsLeader(peer) && Leader(peer) < kMaxNumPeers;
}

CommitResponse SendCommit(MultiPaxosRPC::Stub* stub,
                          int64_t ballot,
                          int64_t last_executed,
                          int64_t global_last_executed) {
  ClientContext context;
  CommitRequest request;
  CommitResponse response;

  request.set_ballot(ballot);
  request.set_last_executed(last_executed);
  request.set_global_last_executed(global_last_executed);
  stub->Commit(&context, request, &response);
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

class MultiPaxosTest : public testing::Test {
 public:
  MultiPaxosTest() {
    for (auto id = 0; id < kNumPeers; ++id) {
      auto config = json::parse(MakeConfig(id, kNumPeers));
      auto log = std::make_unique<Log>(std::make_unique<kvstore::MemKVStore>());
      auto peer = std::make_unique<MultiPaxos>(log.get(), config);

      configs_.push_back(config);
      logs_.push_back(std::move(log));
      peers_.push_back(std::move(peer));
    }
  }

  std::optional<int> OneLeader() const {
    auto leader = Leader(*peers_[0]);
    auto num_leaders = 0;
    for (auto const& p : peers_)
      if (IsLeader(*p)) {
        ++num_leaders;
        if (num_leaders > 1 || p->Id() != leader)
          return std::nullopt;
      } else if (Leader(*p) != leader) {
        return std::nullopt;
      }
    return leader;
  }

 protected:
  std::vector<json> configs_;
  std::vector<std::unique_ptr<Log>> logs_;
  std::vector<std::unique_ptr<MultiPaxos>> peers_;
  std::vector<std::unique_ptr<kvstore::KVStore>> stores_;
};

TEST_F(MultiPaxosTest, Constructor) {
  EXPECT_EQ(kMaxNumPeers, Leader(*peers_[0]));
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_FALSE(IsSomeoneElseLeader(*peers_[0]));
}

TEST_F(MultiPaxosTest, NextBallot) {
  for (int id = 0; id < kNumPeers; ++id) {
    auto ballot = id;
    ballot += kRoundIncrement;
    EXPECT_EQ(ballot, peers_[id]->NextBallot());
  }
}

TEST_F(MultiPaxosTest, RequestsWithLowerBallotIgnored) {
  peers_[0]->StartRPCServer();
  auto stub = MakeStub(configs_[0]["peers"][0]);

  peers_[0]->BecomeLeader(peers_[0]->NextBallot(), logs_[0]->LastIndex());
  peers_[0]->BecomeLeader(peers_[0]->NextBallot(), logs_[0]->LastIndex());
  auto stale_ballot = peers_[1]->NextBallot();

  auto r1 = SendPrepare(stub.get(), stale_ballot);
  EXPECT_EQ(REJECT, r1.type());
  EXPECT_TRUE(IsLeader(*peers_[0]));

  auto index = logs_[0]->AdvanceLastIndex();
  auto instance = MakeInstance(stale_ballot, index);
  auto r2 = SendAccept(stub.get(), instance);
  EXPECT_EQ(REJECT, r2.type());
  EXPECT_TRUE(IsLeader(*peers_[0]));
  EXPECT_EQ(nullptr, logs_[0]->at(index));

  auto r3 = SendCommit(stub.get(), stale_ballot, 0, 0);
  EXPECT_EQ(REJECT, r3.type());
  EXPECT_TRUE(IsLeader(*peers_[0]));

  peers_[0]->StopRPCServer();
}

TEST_F(MultiPaxosTest, RequestsWithHigherBallotChangeLeaderToFollower) {
  peers_[0]->StartRPCServer();
  auto stub = MakeStub(configs_[0]["peers"][0]);

  peers_[0]->BecomeLeader(peers_[0]->NextBallot(), logs_[0]->LastIndex());
  EXPECT_TRUE(IsLeader(*peers_[0]));
  auto r1 = SendPrepare(stub.get(), peers_[1]->NextBallot());
  EXPECT_EQ(OK, r1.type());
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(1, Leader(*peers_[0]));

  peers_[1]->BecomeLeader(peers_[1]->NextBallot(), logs_[1]->LastIndex());

  peers_[0]->BecomeLeader(peers_[0]->NextBallot(), logs_[0]->LastIndex());
  EXPECT_TRUE(IsLeader(*peers_[0]));
  auto index = logs_[0]->AdvanceLastIndex();
  auto instance = MakeInstance(peers_[1]->NextBallot(), index);
  auto r2 = SendAccept(stub.get(), instance);
  EXPECT_EQ(OK, r2.type());
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(1, Leader(*peers_[0]));

  peers_[1]->BecomeLeader(peers_[1]->NextBallot(), logs_[1]->LastIndex());

  peers_[0]->BecomeLeader(peers_[0]->NextBallot(), logs_[0]->LastIndex());
  EXPECT_TRUE(IsLeader(*peers_[0]));
  auto r3 = SendCommit(stub.get(), peers_[1]->NextBallot(), 0, 0);
  EXPECT_EQ(OK, r3.type());
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(1, Leader(*peers_[0]));

  peers_[0]->StopRPCServer();
}

TEST_F(MultiPaxosTest, CommitCommitsAndTrims) {
  peers_[0]->StartRPCServer();
  auto stub = MakeStub(configs_[0]["peers"][0]);

  auto ballot = peers_[0]->NextBallot();
  auto index1 = logs_[0]->AdvanceLastIndex();
  logs_[0]->Append(MakeInstance(ballot, index1));
  auto index2 = logs_[0]->AdvanceLastIndex();
  logs_[0]->Append(MakeInstance(ballot, index2));
  auto index3 = logs_[0]->AdvanceLastIndex();
  logs_[0]->Append(MakeInstance(ballot, index3));

  auto r1 = SendCommit(stub.get(), ballot, index2, 0);
  EXPECT_EQ(OK, r1.type());
  EXPECT_EQ(0, r1.last_executed());
  EXPECT_TRUE(IsCommitted(*logs_[0]->at(index1)));
  EXPECT_TRUE(IsCommitted(*logs_[0]->at(index2)));
  EXPECT_TRUE(IsInProgress(*logs_[0]->at(index3)));

  logs_[0]->Execute();
  logs_[0]->Execute();

  auto r2 = SendCommit(stub.get(), ballot, index2, index2);
  EXPECT_EQ(OK, r2.type());
  EXPECT_EQ(index2, r2.last_executed());
  EXPECT_EQ(nullptr, logs_[0]->at(index1));
  EXPECT_EQ(nullptr, logs_[0]->at(index2));
  EXPECT_TRUE(IsInProgress(*logs_[0]->at(index3)));

  peers_[0]->StopRPCServer();
}

TEST_F(MultiPaxosTest, PrepareRespondsWithCorrectInstances) {
  peers_[0]->StartRPCServer();
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

  auto r2 = SendCommit(stub.get(), ballot, index2, 0);
  EXPECT_EQ(OK, r2.type());

  logs_[0]->Execute();
  logs_[0]->Execute();

  ballot = peers_[0]->NextBallot();

  auto r3 = SendPrepare(stub.get(), ballot);
  EXPECT_EQ(OK, r3.type());
  EXPECT_EQ(3, r3.instances_size());
  EXPECT_TRUE(IsExecuted(r3.instances(0)));
  EXPECT_TRUE(IsExecuted(r3.instances(1)));
  EXPECT_TRUE(IsInProgress(r3.instances(2)));

  auto r4 = SendCommit(stub.get(), ballot, index2, 2);
  EXPECT_EQ(OK, r4.type());

  ballot = peers_[0]->NextBallot();

  auto r5 = SendPrepare(stub.get(), ballot);
  EXPECT_EQ(OK, r5.type());
  EXPECT_EQ(1, r5.instances_size());
  EXPECT_EQ(instance3, r5.instances(0));

  peers_[0]->StopRPCServer();
}

TEST_F(MultiPaxosTest, AcceptAppendsToLog) {
  peers_[0]->StartRPCServer();
  auto stub = MakeStub(configs_[0]["peers"][0]);

  auto ballot = peers_[0]->NextBallot();
  auto index1 = logs_[0]->AdvanceLastIndex();
  auto instance1 = MakeInstance(ballot, index1);
  auto index2 = logs_[0]->AdvanceLastIndex();
  auto instance2 = MakeInstance(ballot, index2);

  auto r1 = SendAccept(stub.get(), instance1);
  EXPECT_EQ(OK, r1.type());
  EXPECT_EQ(instance1, *logs_[0]->at(index1));
  EXPECT_EQ(nullptr, logs_[0]->at(index2));

  auto r2 = SendAccept(stub.get(), instance2);
  EXPECT_EQ(OK, r2.type());
  EXPECT_EQ(instance1, *logs_[0]->at(index1));
  EXPECT_EQ(instance2, *logs_[0]->at(index2));

  peers_[0]->StopRPCServer();
}

TEST_F(MultiPaxosTest, PrepareResponseWithHigherBallotChangesLeaderToFollower) {
  peers_[0]->StartRPCServer();
  peers_[1]->StartRPCServer();
  peers_[2]->StartRPCServer();
  auto stub1 = MakeStub(configs_[0]["peers"][1]);

  auto peer0_ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(peer0_ballot, logs_[0]->LastIndex());
  peers_[1]->BecomeLeader(peers_[1]->NextBallot(), logs_[1]->LastIndex());
  auto peer2_ballot = peers_[2]->NextBallot();
  peers_[2]->BecomeLeader(peer2_ballot, logs_[2]->LastIndex());
  peer2_ballot = peers_[2]->NextBallot();
  peers_[2]->BecomeLeader(peer2_ballot, logs_[2]->LastIndex());

  auto r = SendCommit(stub1.get(), peer2_ballot, 0, 0);
  EXPECT_EQ(OK, r.type());
  EXPECT_FALSE(IsLeader(*peers_[1]));
  EXPECT_EQ(2, Leader(*peers_[1]));

  EXPECT_TRUE(IsLeader(*peers_[0]));
  peer0_ballot = peers_[0]->NextBallot();
  peers_[0]->RunPreparePhase(peer0_ballot);
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(2, Leader(*peers_[0]));

  peers_[0]->StopRPCServer();
  peers_[1]->StopRPCServer();
  peers_[2]->StopRPCServer();
}

TEST_F(MultiPaxosTest, AcceptResponseWithHigherBallotChangesLeaderToFollower) {
  peers_[0]->StartRPCServer();
  peers_[1]->StartRPCServer();
  peers_[2]->StartRPCServer();
  auto stub1 = MakeStub(configs_[0]["peers"][1]);

  auto peer0_ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(peer0_ballot, logs_[0]->LastIndex());
  peers_[1]->BecomeLeader(peers_[1]->NextBallot(), logs_[1]->LastIndex());
  auto peer2_ballot = peers_[2]->NextBallot();
  peers_[2]->BecomeLeader(peer2_ballot, logs_[2]->LastIndex());

  auto cr = SendCommit(stub1.get(), peer2_ballot, 0, 0);
  EXPECT_EQ(OK, cr.type());
  EXPECT_FALSE(IsLeader(*peers_[1]));
  EXPECT_EQ(2, Leader(*peers_[1]));

  EXPECT_TRUE(IsLeader(*peers_[0]));
  auto ar = peers_[0]->RunAcceptPhase(peer0_ballot, 1, Command(), 0);
  EXPECT_EQ(ResultType::kSomeoneElseLeader, ar.type_);
  EXPECT_EQ(2, *ar.leader_);
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(2, Leader(*peers_[0]));

  peers_[0]->StopRPCServer();
  peers_[1]->StopRPCServer();
  peers_[2]->StopRPCServer();
}

TEST_F(MultiPaxosTest, CommitResponseWithHigherBallotChangesLeaderToFollower) {
  peers_[0]->StartRPCServer();
  peers_[1]->StartRPCServer();
  peers_[2]->StartRPCServer();
  auto stub1 = MakeStub(configs_[0]["peers"][1]);

  auto peer0_ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(peer0_ballot, logs_[0]->LastIndex());
  peers_[1]->BecomeLeader(peers_[1]->NextBallot(), logs_[1]->LastIndex());
  auto peer2_ballot = peers_[2]->NextBallot();
  peers_[2]->BecomeLeader(peer2_ballot, logs_[2]->LastIndex());

  auto r = SendCommit(stub1.get(), peer2_ballot, 0, 0);
  EXPECT_EQ(OK, r.type());
  EXPECT_FALSE(IsLeader(*peers_[1]));
  EXPECT_EQ(2, Leader(*peers_[1]));

  EXPECT_TRUE(IsLeader(*peers_[0]));
  peers_[0]->RunCommitPhase(peer0_ballot, 0);
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(2, Leader(*peers_[0]));

  peers_[0]->StopRPCServer();
  peers_[1]->StopRPCServer();
  peers_[2]->StopRPCServer();
}

TEST_F(MultiPaxosTest, RunPreparePhase) {
  peers_[0]->StartRPCServer();

  auto peer0_ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(peer0_ballot, logs_[0]->LastIndex());
  auto peer1_ballot = peers_[1]->NextBallot();
  peers_[1]->BecomeLeader(peer1_ballot, logs_[1]->LastIndex());

  auto index1 = 1;
  auto i1 = MakeInstance(peer0_ballot, index1, PUT);

  logs_[0]->Append(i1);
  logs_[1]->Append(i1);

  auto index2 = 2;
  auto i2 = MakeInstance(peer0_ballot, index2);

  logs_[1]->Append(i2);

  auto index3 = 3;
  auto peer0_i3 = MakeInstance(peer0_ballot, index3, COMMITTED, DEL);
  auto peer1_i3 = MakeInstance(peer1_ballot, index3, INPROGRESS, DEL);

  logs_[0]->Append(peer0_i3);
  logs_[1]->Append(peer1_i3);

  auto index4 = 4;
  auto peer0_i4 = MakeInstance(peer0_ballot, index4, EXECUTED, DEL);
  auto peer1_i4 = MakeInstance(peer1_ballot, index4, INPROGRESS, DEL);

  logs_[0]->Append(peer0_i4);
  logs_[1]->Append(peer1_i4);

  auto index5 = 5;
  peer0_ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(peer0_ballot, logs_[0]->LastIndex());
  peer1_ballot = peers_[1]->NextBallot();
  peers_[1]->BecomeLeader(peer1_ballot, logs_[1]->LastIndex());

  auto peer0_i5 = MakeInstance(peer0_ballot, index5, INPROGRESS, GET);
  auto peer1_i5 = MakeInstance(peer1_ballot, index5, INPROGRESS, PUT);

  logs_[0]->Append(peer0_i5);
  logs_[1]->Append(peer1_i5);

  auto ballot = peers_[0]->NextBallot();

  EXPECT_EQ(std::nullopt, peers_[0]->RunPreparePhase(ballot));

  peers_[1]->StartRPCServer();

  std::this_thread::sleep_for(seconds(2));

  ballot = peers_[0]->NextBallot();

  auto [last_index, log] = *peers_[0]->RunPreparePhase(ballot);

  EXPECT_EQ(index5, last_index);
  EXPECT_EQ(i1, log[index1]);
  EXPECT_EQ(i2, log[index2]);
  EXPECT_EQ(peer0_i3.command(), log[index3].command());
  EXPECT_EQ(peer0_i4.command(), log[index4].command());
  EXPECT_EQ(peer1_i5, log[index5]);

  peers_[0]->StopRPCServer();
  peers_[1]->StopRPCServer();
}

TEST_F(MultiPaxosTest, RunAcceptPhase) {
  peers_[0]->StartRPCServer();

  auto ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(ballot, logs_[0]->LastIndex());
  auto index = logs_[0]->AdvanceLastIndex();

  auto result = peers_[0]->RunAcceptPhase(ballot, index, Command(), 0);

  EXPECT_EQ(ResultType::kRetry, result.type_);
  EXPECT_EQ(std::nullopt, result.leader_);

  EXPECT_TRUE(IsInProgress(*logs_[0]->at(index)));
  EXPECT_EQ(nullptr, logs_[1]->at(index));
  EXPECT_EQ(nullptr, logs_[2]->at(index));

  peers_[1]->StartRPCServer();

  std::this_thread::sleep_for(seconds(2));

  result = peers_[0]->RunAcceptPhase(ballot, index, Command(), 0);

  EXPECT_EQ(ResultType::kOk, result.type_);
  EXPECT_EQ(std::nullopt, result.leader_);

  EXPECT_TRUE(IsCommitted(*logs_[0]->at(index)));
  EXPECT_TRUE(IsInProgress(*logs_[1]->at(index)));
  EXPECT_EQ(nullptr, logs_[2]->at(index));

  peers_[0]->StopRPCServer();
  peers_[1]->StopRPCServer();
}

TEST_F(MultiPaxosTest, RunCommitPhase) {
  peers_[0]->StartRPCServer();
  peers_[1]->StartRPCServer();

  auto ballot = peers_[0]->NextBallot();

  for (size_t index = 1; index <= 3; ++index) {
    for (size_t peer = 0; peer < kNumPeers; ++peer) {
      if (index == 3 && peer == 2)
        continue;
      logs_[peer]->Append(MakeInstance(ballot, index, COMMITTED));
      logs_[peer]->Execute();
    }
  }

  auto gle = 0;
  gle = peers_[0]->RunCommitPhase(ballot, gle);
  EXPECT_EQ(0, gle);

  peers_[2]->StartRPCServer();

  logs_[2]->Append(MakeInstance(ballot, 3));

  std::this_thread::sleep_for(seconds(2));

  gle = peers_[0]->RunCommitPhase(ballot, gle);
  EXPECT_EQ(2, gle);

  logs_[2]->Execute();

  gle = peers_[0]->RunCommitPhase(ballot, gle);
  EXPECT_EQ(3, gle);

  peers_[0]->StopRPCServer();
  peers_[1]->StopRPCServer();
  peers_[2]->StopRPCServer();
}

TEST_F(MultiPaxosTest, Replay) {
  peers_[0]->StartRPCServer();
  peers_[1]->StartRPCServer();

  auto ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(ballot, logs_[0]->LastIndex());

  auto index1 = 1;
  auto i1 = MakeInstance(ballot, index1, COMMITTED, PUT);
  auto index2 = 2;
  auto i2 = MakeInstance(ballot, index2, EXECUTED, GET);
  auto index3 = 3;
  auto i3 = MakeInstance(ballot, index3, INPROGRESS, DEL);
  std::unordered_map<int64_t, multipaxos::Instance> log{
      {index1, i1}, {index2, i2}, {index3, i3}};

  EXPECT_EQ(nullptr, logs_[0]->at(index1));
  EXPECT_EQ(nullptr, logs_[0]->at(index2));
  EXPECT_EQ(nullptr, logs_[0]->at(index3));

  EXPECT_EQ(nullptr, logs_[1]->at(index1));
  EXPECT_EQ(nullptr, logs_[1]->at(index2));
  EXPECT_EQ(nullptr, logs_[1]->at(index3));

  auto new_ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(new_ballot, logs_[0]->LastIndex());
  peers_[0]->Replay(new_ballot, log);

  i1.set_ballot(new_ballot);
  i2.set_ballot(new_ballot);
  i3.set_ballot(new_ballot);

  i1.set_state(COMMITTED);
  i2.set_state(COMMITTED);
  i3.set_state(COMMITTED);

  EXPECT_EQ(i1, *logs_[0]->at(index1));
  EXPECT_EQ(i2, *logs_[0]->at(index2));
  EXPECT_EQ(i3, *logs_[0]->at(index3));

  i1.set_state(INPROGRESS);
  i2.set_state(INPROGRESS);
  i3.set_state(INPROGRESS);

  EXPECT_EQ(i1, *logs_[1]->at(index1));
  EXPECT_EQ(i2, *logs_[1]->at(index2));
  EXPECT_EQ(i3, *logs_[1]->at(index3));

  peers_[0]->StopRPCServer();
  peers_[1]->StopRPCServer();
}

TEST_F(MultiPaxosTest, Replicate) {
  peers_[0]->Start();

  auto result = peers_[0]->Replicate(Command(), 0);
  EXPECT_EQ(ResultType::kRetry, result.type_);
  EXPECT_EQ(std::nullopt, result.leader_);

  peers_[1]->Start();
  peers_[2]->Start();

  int commit_interval = configs_[0]["commit_interval"];
  auto commit_interval_3x = 3 * milliseconds(commit_interval);

  std::this_thread::sleep_for(commit_interval_3x);

  auto leader = OneLeader();
  EXPECT_NE(std::nullopt, leader);

  result = peers_[*leader]->Replicate(Command(), 0);
  EXPECT_EQ(ResultType::kOk, result.type_);
  EXPECT_EQ(std::nullopt, result.leader_);

  auto nonleader = (*leader + 1) % kNumPeers;

  result = peers_[nonleader]->Replicate(Command(), 0);
  EXPECT_EQ(ResultType::kSomeoneElseLeader, result.type_);
  EXPECT_EQ(leader, *result.leader_);

  peers_[0]->Stop();
  peers_[1]->Stop();
  peers_[2]->Stop();
}
