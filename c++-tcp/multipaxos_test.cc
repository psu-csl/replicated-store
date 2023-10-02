#include <asio.hpp>
#include <gtest/gtest.h>
#include <chrono>
#include <memory>
#include <thread>

#include "json.h"
#include "log.h"
#include "memkvstore.h"
#include "multipaxos.h"
#include "peer_server.h"
#include "test_util.h"

using namespace std::chrono;

using asio::ip::tcp;
using nlohmann::json;

static const int kNumPeers = 3;
static std::vector<bool> is_server_on_(3);
// static std::vector<tcp::socket*> sockets;

int64_t Leader(MultiPaxos const& peer) {
  return ExtractLeaderId(peer.Ballot());
}

bool IsLeader(MultiPaxos const& peer) {
  return IsLeader(peer.Ballot(), peer.Id());
}

bool IsSomeoneElseLeader(MultiPaxos const& peer) {
  return !IsLeader(peer) && Leader(peer) < kMaxNumPeers;
}

json SendRequest(MultiPaxos& peer, 
                 MessageType msg_type,
                 int target_id,
                 json const& request) {
  int64_t channel_id = peer.NextChannelId();
  auto response_channel = peer.AddChannel(channel_id);
  peer.Stub(target_id).stub_->SendAwaitResponse(msg_type,
                                                channel_id,
                                                request.dump());
  std::string response_str;
  response_channel.wait_dequeue(response_str);
  json response = json::parse(response_str);
  return response;
}

CommitResponse SendCommit(MultiPaxos& peer,
                          int target_id,
                          int64_t ballot,
                          int64_t last_executed,
                          int64_t global_last_executed) {
  CommitResponse commit_response(REJECT, ballot, last_executed);
  if (!is_server_on_[target_id]) {
    return commit_response;
  }

  CommitRequest commit_request(ballot, last_executed, 
                               global_last_executed, peer.Id());
  json request = commit_request;
  json response = SendRequest(peer, COMMITREQUEST, target_id, request);
  commit_response = response.template get<CommitResponse>();
  return commit_response;
}

PrepareResponse SendPrepare(MultiPaxos& peer, int target_id, int64_t ballot) {
  PrepareResponse prepare_response(REJECT, ballot, std::vector<Instance>());
  if (!is_server_on_[target_id]) {
    return prepare_response;
  }

  PrepareRequest prepare_request(ballot, peer.Id());
  json request = prepare_request;
  json response = SendRequest(peer, PREPAREREQUEST, target_id, request);
  prepare_response = response.template get<PrepareResponse>();
  return prepare_response;
}

AcceptResponse SendAccept(MultiPaxos& peer, 
                          int target_id, 
                          Instance const& instance) {
  AcceptResponse accept_response(REJECT, 0);
  if (!is_server_on_[target_id]) {
    return accept_response;
  }

  AcceptRequest accept_request(instance, peer.Id());
  json request = accept_request;
  json response = SendRequest(peer, ACCEPTREQUEST, target_id, request);
  accept_response = response.template get<AcceptResponse>();
  return accept_response;
}

class MultiPaxosTest : public testing::Test {
 public:
  MultiPaxosTest() {
    is_io_context_enabled_ = false;
    for (auto id = 0; id < kNumPeers; ++id) {
      auto config = json::parse(MakeConfig(id, kNumPeers));
      configs_.push_back(config);
      is_server_on_[id] = false;
      std::string ip_port = config["peers"][id];
      auto server = std::make_shared<PeerServer>(id, ip_port, &io_context_,
                                                 config["peers"].size(), 1);
      peer_servers_.push_back(std::move(server));
    }

    for (auto id = 0; id < kNumPeers; ++id) {
      auto log = std::make_unique<Log>(std::make_unique<kvstore::MemKVStore>());
      auto peer = std::make_unique<MultiPaxos>(
          log.get(), configs_[id], &io_context_);
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

  void StartPeerConnection(int id) {
    peer_servers_[id].get()->StartServer(peers_[id].get());
    is_server_on_[id] = true;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    if (!is_io_context_enabled_) {
      is_io_context_enabled_ = true;
      t = std::thread([this] { io_context_.run(); });
    }
  }

  void StopServer() {
    for (auto id = 0; id < kNumPeers; ++id) {
      if (is_server_on_[id])
        peer_servers_[id].get()->StopServer();
    }
    for (auto id = 0; id < kNumPeers; ++id) {
      peers_[id].get()->StopStub();
    }
    if (is_io_context_enabled_) {
      io_context_.stop();
      t.join();
    }
  }

 protected:
  std::vector<json> configs_;
  std::vector<std::unique_ptr<Log>> logs_;
  std::vector<std::unique_ptr<MultiPaxos>> peers_;
  std::vector<std::unique_ptr<kvstore::KVStore>> stores_;
  // std::vector<std::unique_ptr<tcp::acceptor>> acceptors_;
  std::vector<std::shared_ptr<PeerServer>> peer_servers_;
  // std::vector<std::thread> threads_;
  std::thread t;
  asio::io_context io_context_;
  std::atomic<bool> is_io_context_enabled_;
};

TEST_F(MultiPaxosTest, Constructor) {
  EXPECT_EQ(kMaxNumPeers, Leader(*peers_[0]));
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_FALSE(IsSomeoneElseLeader(*peers_[0]));
  StopServer();
}

TEST_F(MultiPaxosTest, NextBallot) {
  for (int id = 0; id < kNumPeers; ++id) {
    auto ballot = id;
    ballot += kRoundIncrement;
    EXPECT_EQ(ballot, peers_[id]->NextBallot());
  }
  StopServer();
}

TEST_F(MultiPaxosTest, RequestsWithLowerBallotIgnored) {
  StartPeerConnection(0);

  peers_[0]->BecomeLeader(peers_[0]->NextBallot(), logs_[0]->LastIndex());
  peers_[0]->BecomeLeader(peers_[0]->NextBallot(), logs_[0]->LastIndex());
  auto stale_ballot = peers_[1]->NextBallot();

  auto r1 = SendPrepare(*peers_[1], 0, stale_ballot);
  EXPECT_EQ(REJECT, r1.type_);
  EXPECT_TRUE(IsLeader(*peers_[0]));

  auto index = logs_[0]->AdvanceLastIndex();
  auto instance = MakeInstance(stale_ballot, index);
  auto r2 = SendAccept(*peers_[1], 0, instance);
  EXPECT_EQ(REJECT, r2.type_);
  EXPECT_TRUE(IsLeader(*peers_[0]));
  EXPECT_EQ(nullptr, logs_[0]->at(index));

  auto r3 = SendCommit(*peers_[1], 0, stale_ballot, 0, 0);
  EXPECT_EQ(REJECT, r3.type_);
  EXPECT_TRUE(IsLeader(*peers_[0]));

  StopServer();
}

TEST_F(MultiPaxosTest, RequestsWithHigherBallotChangeLeaderToFollower) {
  StartPeerConnection(0);

  peers_[0]->BecomeLeader(peers_[0]->NextBallot(), logs_[0]->LastIndex());
  EXPECT_TRUE(IsLeader(*peers_[0]));
  auto r1 = SendPrepare(*peers_[1], 0, peers_[1]->NextBallot());
  EXPECT_EQ(OK, r1.type_);
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(1, Leader(*peers_[0]));

  peers_[1]->BecomeLeader(peers_[1]->NextBallot(), logs_[1]->LastIndex());

  peers_[0]->BecomeLeader(peers_[0]->NextBallot(), logs_[0]->LastIndex());
  EXPECT_TRUE(IsLeader(*peers_[0]));
  auto index = logs_[0]->AdvanceLastIndex();
  auto instance = MakeInstance(peers_[1]->NextBallot(), index);
  auto r2 = SendAccept(*peers_[1], 0, instance);
  EXPECT_EQ(OK, r2.type_);
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(1, Leader(*peers_[0]));

  peers_[1]->BecomeLeader(peers_[1]->NextBallot(), logs_[1]->LastIndex());

  peers_[0]->BecomeLeader(peers_[0]->NextBallot(), logs_[0]->LastIndex());
  EXPECT_TRUE(IsLeader(*peers_[0]));
  auto r3 = SendCommit(*peers_[1], 0, peers_[1]->NextBallot(), 0, 0);
  EXPECT_EQ(OK, r3.type_);
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(1, Leader(*peers_[0]));

  StopServer();
}

TEST_F(MultiPaxosTest, CommitCommitsAndTrims) {
  StartPeerConnection(0);

  auto ballot = peers_[0]->NextBallot();
  auto index1 = logs_[0]->AdvanceLastIndex();
  logs_[0]->Append(MakeInstance(ballot, index1));
  auto index2 = logs_[0]->AdvanceLastIndex();
  logs_[0]->Append(MakeInstance(ballot, index2));
  auto index3 = logs_[0]->AdvanceLastIndex();
  logs_[0]->Append(MakeInstance(ballot, index3));

  auto r1 = SendCommit(*peers_[1], 0, ballot, index2, 0);
  EXPECT_EQ(OK, r1.type_);
  EXPECT_EQ(0, r1.last_executed_);
  EXPECT_TRUE(IsCommitted(*logs_[0]->at(index1)));
  EXPECT_TRUE(IsCommitted(*logs_[0]->at(index2)));
  EXPECT_TRUE(IsInProgress(*logs_[0]->at(index3)));

  logs_[0]->Execute();
  logs_[0]->Execute();

  auto r2 = SendCommit(*peers_[1], 0, ballot, index2, index2);
  EXPECT_EQ(OK, r2.type_);
  EXPECT_EQ(index2, r2.last_executed_);
  EXPECT_EQ(nullptr, logs_[0]->at(index1));
  EXPECT_EQ(nullptr, logs_[0]->at(index2));
  EXPECT_TRUE(IsInProgress(*logs_[0]->at(index3)));

  StopServer();
}

TEST_F(MultiPaxosTest, PrepareRespondsWithCorrectInstances) {
  StartPeerConnection(0);

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

  auto r1 = SendPrepare(*peers_[1], 0, ballot);
  EXPECT_EQ(OK, r1.type_);
  EXPECT_EQ(3, r1.instances_.size());
  EXPECT_EQ(instance1, r1.instances_[0]);
  EXPECT_EQ(instance2, r1.instances_[1]);
  EXPECT_EQ(instance3, r1.instances_[2]);

  auto r2 = SendCommit(*peers_[1], 0, ballot, index2, 0);
  EXPECT_EQ(OK, r2.type_);

  logs_[0]->Execute();
  logs_[0]->Execute();

  ballot = peers_[0]->NextBallot();

  auto r3 = SendPrepare(*peers_[1], 0, ballot);
  EXPECT_EQ(OK, r3.type_);
  EXPECT_EQ(3, r3.instances_.size());
  EXPECT_TRUE(IsExecuted(r3.instances_[0]));
  EXPECT_TRUE(IsExecuted(r3.instances_[1]));
  EXPECT_TRUE(IsInProgress(r3.instances_[2]));

  auto r4 = SendCommit(*peers_[1], 0, ballot, index2, 2);
  EXPECT_EQ(OK, r4.type_);

  ballot = peers_[0]->NextBallot();

  auto r5 = SendPrepare(*peers_[1], 0, ballot);
  EXPECT_EQ(OK, r5.type_);
  EXPECT_EQ(1, r5.instances_.size());
  EXPECT_EQ(instance3, r5.instances_[0]);

  StopServer();
}

TEST_F(MultiPaxosTest, AcceptAppendsToLog) {
  StartPeerConnection(0);

  auto ballot = peers_[0]->NextBallot();
  auto index1 = logs_[0]->AdvanceLastIndex();
  auto instance1 = MakeInstance(ballot, index1);
  auto index2 = logs_[0]->AdvanceLastIndex();
  auto instance2 = MakeInstance(ballot, index2);

  auto r1 = SendAccept(*peers_[1], 0, instance1);
  EXPECT_EQ(OK, r1.type_);
  EXPECT_EQ(instance1, *logs_[0]->at(index1));
  EXPECT_EQ(nullptr, logs_[0]->at(index2));

  auto r2 = SendAccept(*peers_[1], 0, instance2);
  EXPECT_EQ(OK, r2.type_);
  EXPECT_EQ(instance1, *logs_[0]->at(index1));
  EXPECT_EQ(instance2, *logs_[0]->at(index2));

  StopServer();
}

TEST_F(MultiPaxosTest, PrepareResponseWithHigherBallotChangesLeaderToFollower) {
  StartPeerConnection(0);
  StartPeerConnection(1);
  StartPeerConnection(2);

  auto peer0_ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(peer0_ballot, logs_[0]->LastIndex());
  peers_[1]->BecomeLeader(peers_[1]->NextBallot(), logs_[1]->LastIndex());
  auto peer2_ballot = peers_[2]->NextBallot();
  peers_[2]->BecomeLeader(peer2_ballot, logs_[2]->LastIndex());
  peer2_ballot = peers_[2]->NextBallot();
  peers_[2]->BecomeLeader(peer2_ballot, logs_[2]->LastIndex());

  auto r = SendCommit(*peers_[0], 1, peer2_ballot, 0, 0);
  EXPECT_EQ(OK, r.type_);
  EXPECT_FALSE(IsLeader(*peers_[1]));
  EXPECT_EQ(2, Leader(*peers_[1]));

  EXPECT_TRUE(IsLeader(*peers_[0]));
  peer0_ballot = peers_[0]->NextBallot();
  peers_[0]->RunPreparePhase(peer0_ballot);
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(2, Leader(*peers_[0]));

  StopServer();
}

TEST_F(MultiPaxosTest, AcceptResponseWithHigherBallotChangesLeaderToFollower) {
  StartPeerConnection(0);
  StartPeerConnection(1);
  StartPeerConnection(2);

  auto peer0_ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(peer0_ballot, logs_[0]->LastIndex());
  peers_[1]->BecomeLeader(peers_[1]->NextBallot(), logs_[1]->LastIndex());
  auto peer2_ballot = peers_[2]->NextBallot();
  peers_[2]->BecomeLeader(peer2_ballot, logs_[2]->LastIndex());

  auto cr = SendCommit(*peers_[0], 1, peer2_ballot, 0, 0);
  EXPECT_EQ(OK, cr.type_);
  EXPECT_FALSE(IsLeader(*peers_[1]));
  EXPECT_EQ(2, Leader(*peers_[1]));

  EXPECT_TRUE(IsLeader(*peers_[0]));
  auto ar = peers_[0]->RunAcceptPhase(peer0_ballot, 1, Command(), 0);
  EXPECT_EQ(ResultType::kSomeoneElseLeader, ar.type_);
  EXPECT_EQ(2, *ar.leader_);
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(2, Leader(*peers_[0]));

  StopServer();
}

TEST_F(MultiPaxosTest, CommitResponseWithHigherBallotChangesLeaderToFollower) {
  StartPeerConnection(0);
  StartPeerConnection(1);
  StartPeerConnection(2);

  auto peer0_ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(peer0_ballot, logs_[0]->LastIndex());
  peers_[1]->BecomeLeader(peers_[1]->NextBallot(), logs_[1]->LastIndex());
  auto peer2_ballot = peers_[2]->NextBallot();
  peers_[2]->BecomeLeader(peer2_ballot, logs_[2]->LastIndex());

  auto r = SendCommit(*peers_[0], 1, peer2_ballot, 0, 0);
  EXPECT_EQ(OK, r.type_);
  EXPECT_FALSE(IsLeader(*peers_[1]));
  EXPECT_EQ(2, Leader(*peers_[1]));

  EXPECT_TRUE(IsLeader(*peers_[0]));
  peers_[0]->RunCommitPhase(peer0_ballot, 0);
  EXPECT_FALSE(IsLeader(*peers_[0]));
  EXPECT_EQ(2, Leader(*peers_[0]));

  StopServer();
}

TEST_F(MultiPaxosTest, RunPreparePhase) {
  StartPeerConnection(0);

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

  StartPeerConnection(1);

  std::this_thread::sleep_for(seconds(2));

  ballot = peers_[0]->NextBallot();

  auto [last_index, log] = *peers_[0]->RunPreparePhase(ballot);

  EXPECT_EQ(index5, last_index);
  EXPECT_EQ(i1, log[index1]);
  EXPECT_EQ(i2, log[index2]);
  EXPECT_EQ(peer0_i3.command_, log[index3].command_);
  EXPECT_EQ(peer0_i4.command_, log[index4].command_);
  EXPECT_EQ(peer1_i5, log[index5]);

  StopServer();
}

TEST_F(MultiPaxosTest, RunAcceptPhase) {
  StartPeerConnection(0);

  auto ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(ballot, logs_[0]->LastIndex());
  auto index = logs_[0]->AdvanceLastIndex();

  auto result = peers_[0]->RunAcceptPhase(ballot, index, Command(), 0);

  EXPECT_EQ(ResultType::kRetry, result.type_);
  EXPECT_EQ(std::nullopt, result.leader_);

  EXPECT_TRUE(IsInProgress(*logs_[0]->at(index)));
  EXPECT_EQ(nullptr, logs_[1]->at(index));
  EXPECT_EQ(nullptr, logs_[2]->at(index));

  StartPeerConnection(1);

  std::this_thread::sleep_for(seconds(2));

  result = peers_[0]->RunAcceptPhase(ballot, index, Command(), 0);

  EXPECT_EQ(ResultType::kOk, result.type_);
  EXPECT_EQ(std::nullopt, result.leader_);

  EXPECT_TRUE(IsCommitted(*logs_[0]->at(index)));
  EXPECT_TRUE(IsInProgress(*logs_[1]->at(index)));
  EXPECT_EQ(nullptr, logs_[2]->at(index));

  StopServer();
}

TEST_F(MultiPaxosTest, RunCommitPhase) {
  StartPeerConnection(0);
  StartPeerConnection(1);

  auto ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(ballot, logs_[0]->LastIndex());

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

  StartPeerConnection(2);

  logs_[2]->Append(MakeInstance(ballot, 3));

  std::this_thread::sleep_for(seconds(2));

  gle = peers_[0]->RunCommitPhase(ballot, gle);
  EXPECT_EQ(2, gle);

  logs_[2]->Execute();

  gle = peers_[0]->RunCommitPhase(ballot, gle);
  EXPECT_EQ(3, gle);

  StopServer();
}

TEST_F(MultiPaxosTest, Replay) {
  StartPeerConnection(0);
  StartPeerConnection(1);

  auto ballot = peers_[0]->NextBallot();
  peers_[0]->BecomeLeader(ballot, logs_[0]->LastIndex());

  auto index1 = 1;
  auto i1 = MakeInstance(ballot, index1, COMMITTED, PUT);
  auto index2 = 2;
  auto i2 = MakeInstance(ballot, index2, EXECUTED, GET);
  auto index3 = 3;
  auto i3 = MakeInstance(ballot, index3, INPROGRESS, DEL);
  std::unordered_map<int64_t, Instance> log{
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

  i1.ballot_ = new_ballot;
  i2.ballot_ = new_ballot;
  i3.ballot_ = new_ballot;

  i1.state_ = COMMITTED;
  i2.state_ = COMMITTED;
  i3.state_ = COMMITTED;

  EXPECT_EQ(i1, *logs_[0]->at(index1));
  EXPECT_EQ(i2, *logs_[0]->at(index2));
  EXPECT_EQ(i3, *logs_[0]->at(index3));

  i1.state_ = INPROGRESS;
  i2.state_ = INPROGRESS;
  i3.state_ = INPROGRESS;

  EXPECT_EQ(i1, *logs_[1]->at(index1));
  EXPECT_EQ(i2, *logs_[1]->at(index2));
  EXPECT_EQ(i3, *logs_[1]->at(index3));

  StopServer();
}

TEST_F(MultiPaxosTest, Replicate) {
  StartPeerConnection(0);
  peers_[0]->Start();

  auto result = peers_[0]->Replicate(Command(), 0);
  EXPECT_EQ(ResultType::kRetry, result.type_);
  EXPECT_EQ(std::nullopt, result.leader_);

  StartPeerConnection(1);
  StartPeerConnection(2);
  peers_[1]->Start();
  peers_[2]->Start();

  int commit_interval = configs_[0]["commit_interval"];
  auto commit_interval_10x = 10 * milliseconds(commit_interval);

  std::this_thread::sleep_for(commit_interval_10x);
  auto leader = OneLeader();
  EXPECT_NE(std::nullopt, leader);

  result = peers_[*leader]->Replicate(Command(), 0);
  EXPECT_EQ(ResultType::kOk, result.type_);
  EXPECT_EQ(std::nullopt, result.leader_);

  auto nonleader = (*leader + 1) % kNumPeers;

  result = peers_[nonleader]->Replicate(Command(), 0);
  EXPECT_EQ(ResultType::kSomeoneElseLeader, result.type_);
  EXPECT_EQ(leader, *result.leader_);

  for (auto id = 0; id < kNumPeers; ++id) 
    peer_servers_[id].get()->StopServer();
  io_context_.stop();
  t.join();

  peers_[0]->Stop();
  peers_[1]->Stop();
  peers_[2]->Stop();

}
