#include <gtest/gtest.h>
#include <thread>

#include "log.h"
#include "memkvstore.h"

using multipaxos::Command;
using multipaxos::CommandType;
using multipaxos::CommandType::DEL;
using multipaxos::CommandType::GET;
using multipaxos::CommandType::PUT;

using multipaxos::Instance;
using multipaxos::InstanceState;
using multipaxos::InstanceState::COMMITTED;
using multipaxos::InstanceState::EXECUTED;
using multipaxos::InstanceState::INPROGRESS;

class LogTest : public testing::Test {
 protected:
  Instance MakeInstance(int64_t ballot) {
    Instance i;
    i.set_ballot(ballot);
    i.set_index(log_.AdvanceLastIndex());
    i.set_state(INPROGRESS);
    *i.mutable_command() = Command();
    return i;
  }
  Instance MakeInstance(int64_t ballot, int64_t index) {
    Instance i;
    i.set_ballot(ballot);
    i.set_index(index);
    i.set_state(INPROGRESS);
    *i.mutable_command() = Command();
    return i;
  }
  Instance MakeInstance(int64_t ballot, InstanceState state) {
    Instance i;
    i.set_ballot(ballot);
    i.set_index(log_.AdvanceLastIndex());
    i.set_state(state);
    *i.mutable_command() = Command();
    return i;
  }
  Instance MakeInstance(int64_t ballot, int64_t index, CommandType type) {
    Instance i;
    i.set_ballot(ballot);
    i.set_index(index);
    i.set_state(INPROGRESS);
    Command c;
    c.set_type(type);
    *i.mutable_command() = c;
    return i;
  }
  Instance MakeInstance(int64_t ballot,
                        int64_t index,
                        InstanceState state,
                        CommandType type) {
    Instance i;
    i.set_ballot(ballot);
    i.set_index(index);
    i.set_state(state);
    Command c;
    c.set_type(type);
    *i.mutable_command() = c;
    return i;
  }

  Log log_;
  MemKVStore store_;
};

using LogDeathTest = LogTest;

TEST_F(LogTest, Constructor) {
  EXPECT_EQ(log_.LastExecuted(), 0);
  EXPECT_EQ(log_.GlobalLastExecuted(), 0);
  EXPECT_FALSE(log_.IsExecutable());
  EXPECT_EQ(nullptr, log_[0]);
  EXPECT_EQ(nullptr, log_[-1]);
  EXPECT_EQ(nullptr, log_[3]);
}

TEST_F(LogTest, Insert) {
  log_t log;
  auto index = 1;
  auto ballot = 1;
  EXPECT_TRUE(Insert(&log, MakeInstance(ballot, index, PUT)));
  EXPECT_EQ(PUT, log[index].command().type());
  EXPECT_FALSE(Insert(&log, MakeInstance(ballot, index, PUT)));
}

TEST_F(LogTest, InsertUpdateInProgress) {
  log_t log;
  auto index = 1;
  auto ballot = 1;
  EXPECT_TRUE(Insert(&log, MakeInstance(ballot, index, PUT)));
  EXPECT_EQ(PUT, log[index].command().type());
  EXPECT_FALSE(Insert(&log, MakeInstance(ballot + 1, index, DEL)));
  EXPECT_EQ(DEL, log[index].command().type());
}

TEST_F(LogTest, InsertUpdateCommitted) {
  log_t log;
  auto index = 1;
  auto ballot = 1;
  EXPECT_TRUE(Insert(&log, MakeInstance(ballot, index, COMMITTED, PUT)));
  EXPECT_FALSE(Insert(&log, MakeInstance(ballot, index, INPROGRESS, PUT)));
}

TEST_F(LogTest, InsertStale) {
  log_t log;
  auto index = 1;
  auto ballot = 1;
  EXPECT_TRUE(Insert(&log, MakeInstance(ballot, index, PUT)));
  EXPECT_EQ(PUT, log[index].command().type());
  EXPECT_FALSE(Insert(&log, MakeInstance(ballot - 1, index, DEL)));
  EXPECT_EQ(PUT, log[index].command().type());
}

TEST_F(LogDeathTest, InsertCase2Committed) {
  auto index = 1;
  auto inst1 = MakeInstance(0, index, COMMITTED, PUT);
  auto inst2 = MakeInstance(0, index, INPROGRESS, DEL);
  log_t log;
  Insert(&log, std::move(inst1));
  EXPECT_DEATH(Insert(&log, std::move(inst2)), "Insert case2");
}

TEST_F(LogDeathTest, InsertCase2Executed) {
  auto index = 1;
  auto inst1 = MakeInstance(0, index, EXECUTED, PUT);
  auto inst2 = MakeInstance(0, index, INPROGRESS, DEL);
  log_t log;
  Insert(&log, std::move(inst1));
  EXPECT_DEATH(Insert(&log, std::move(inst2)), "Insert case2");
}

TEST_F(LogDeathTest, InsertCase3) {
  auto index = 1;
  auto inst1 = MakeInstance(0, index, INPROGRESS, PUT);
  auto inst2 = MakeInstance(0, index, INPROGRESS, DEL);
  log_t log;
  Insert(&log, std::move(inst1));
  EXPECT_DEATH(Insert(&log, std::move(inst2)), "Insert case3");
}

TEST_F(LogTest, Append) {
  log_.Append(MakeInstance(0));
  log_.Append(MakeInstance(0));
  EXPECT_EQ(1, log_[1]->index());
  EXPECT_EQ(2, log_[2]->index());
}

TEST_F(LogTest, AppendWithGap) {
  auto index = 42;
  log_.Append(MakeInstance(0, index));
  EXPECT_EQ(index, log_[index]->index());
  EXPECT_EQ(index + 1, log_.AdvanceLastIndex());
}

TEST_F(LogTest, AppendFillGaps) {
  auto index = 42;
  log_.Append(MakeInstance(0, index));
  log_.Append(MakeInstance(0, index - 10));
  EXPECT_EQ(index + 1, log_.AdvanceLastIndex());
}

TEST_F(LogTest, AppendHighBallotOverride) {
  auto index = 1, lo_ballot = 0, hi_ballot = 1;
  log_.Append(MakeInstance(lo_ballot, index, PUT));
  log_.Append(MakeInstance(hi_ballot, index, DEL));
  EXPECT_EQ(DEL, log_[index]->command().type());
}

TEST_F(LogTest, AppendLowBallotNoEffect) {
  auto index = 1, lo_ballot = 0, hi_ballot = 1;
  log_.Append(MakeInstance(hi_ballot, index, PUT));
  log_.Append(MakeInstance(lo_ballot, index, DEL));
  EXPECT_EQ(PUT, log_[index]->command().type());
}

TEST_F(LogTest, Commit) {
  auto index1 = 1;
  log_.Append(MakeInstance(0, index1));
  auto index2 = 2;
  log_.Append(MakeInstance(0, index2));

  EXPECT_TRUE(IsInProgress(*log_[index1]));
  EXPECT_TRUE(IsInProgress(*log_[index2]));
  EXPECT_FALSE(log_.IsExecutable());

  log_.Commit(index2);

  EXPECT_TRUE(IsInProgress(*log_[index1]));
  EXPECT_TRUE(IsCommitted(*log_[index2]));
  EXPECT_FALSE(log_.IsExecutable());

  log_.Commit(index1);

  EXPECT_TRUE(IsCommitted(*log_[index1]));
  EXPECT_TRUE(IsCommitted(*log_[index2]));
  EXPECT_TRUE(log_.IsExecutable());
}

TEST_F(LogTest, CommitBeforeAppend) {
  auto index = 1;
  std::thread commit_thread([this, index] { log_.Commit(index); });
  std::this_thread::yield();
  log_.Append(MakeInstance(0));
  commit_thread.join();
  EXPECT_TRUE(IsCommitted(*log_[index]));
}

TEST_F(LogTest, AppendCommitExecute) {
  std::thread execute_thread([this] { log_.Execute(&store_); });

  auto index = 1;
  log_.Append(MakeInstance(0, index));
  log_.Commit(index);
  execute_thread.join();

  EXPECT_TRUE(IsExecuted(*log_[index]));
  EXPECT_EQ(index, log_.LastExecuted());
}

TEST_F(LogTest, AppendCommitExecuteOutOfOrder) {
  std::thread execute_thread([this] {
    log_.Execute(&store_);
    log_.Execute(&store_);
    log_.Execute(&store_);
  });

  auto index1 = 1;
  log_.Append(MakeInstance(0, index1));
  auto index2 = 2;
  log_.Append(MakeInstance(0, index2));
  auto index3 = 3;
  log_.Append(MakeInstance(0, index3));

  log_.Commit(index3);
  log_.Commit(index2);
  log_.Commit(index1);

  execute_thread.join();

  EXPECT_TRUE(IsExecuted(*log_[index1]));
  EXPECT_TRUE(IsExecuted(*log_[index2]));
  EXPECT_TRUE(IsExecuted(*log_[index3]));
  EXPECT_EQ(index3, log_.LastExecuted());
}

TEST_F(LogTest, CommitUntil) {
  auto ballot = 0;
  auto index1 = 1;
  log_.Append(MakeInstance(ballot, index1));
  auto index2 = 2;
  log_.Append(MakeInstance(ballot, index2));
  auto index3 = 3;
  log_.Append(MakeInstance(ballot, index3));

  log_.CommitUntil(index2, ballot);

  EXPECT_TRUE(IsCommitted(*log_[index1]));
  EXPECT_TRUE(IsCommitted(*log_[index2]));
  EXPECT_FALSE(IsCommitted(*log_[index3]));
  EXPECT_TRUE(log_.IsExecutable());
}

TEST_F(LogTest, CommitUntilHigherBallot) {
  auto ballot = 0;
  auto index1 = 1;
  log_.Append(MakeInstance(ballot, index1));
  auto index2 = 2;
  log_.Append(MakeInstance(ballot, index2));
  auto index3 = 3;
  log_.Append(MakeInstance(ballot, index3));

  log_.CommitUntil(index3, ballot + 1);

  EXPECT_FALSE(IsCommitted(*log_[index1]));
  EXPECT_FALSE(IsCommitted(*log_[index2]));
  EXPECT_FALSE(IsCommitted(*log_[index3]));
  EXPECT_FALSE(log_.IsExecutable());
}

TEST_F(LogDeathTest, CommitUntilCase2) {
  auto ballot = 5;
  auto index1 = 1;
  log_.Append(MakeInstance(ballot, index1));
  auto index2 = 2;
  log_.Append(MakeInstance(ballot, index2));
  auto index3 = 3;
  log_.Append(MakeInstance(ballot, index3));

  EXPECT_DEATH(log_.CommitUntil(index3, ballot - 1), "CommitUntil case 2");
}

TEST_F(LogTest, CommitUntilWithGap) {
  auto ballot = 0;
  auto index1 = 1;
  log_.Append(MakeInstance(ballot, index1));
  auto index3 = 3;
  log_.Append(MakeInstance(ballot, index3));
  auto index4 = 4;
  log_.Append(MakeInstance(ballot, index4));

  log_.CommitUntil(index4, ballot);

  EXPECT_TRUE(IsCommitted(*log_[index1]));
  EXPECT_FALSE(IsCommitted(*log_[index3]));
  EXPECT_FALSE(IsCommitted(*log_[index4]));
  EXPECT_TRUE(log_.IsExecutable());
}

TEST_F(LogTest, AppendCommitUntilExecute) {
  std::thread execute_thread([this] {
    log_.Execute(&store_);
    log_.Execute(&store_);
    log_.Execute(&store_);
  });

  auto ballot = 0;
  auto index1 = 1;
  log_.Append(MakeInstance(ballot, index1));
  auto index2 = 2;
  log_.Append(MakeInstance(ballot, index2));
  auto index3 = 3;
  log_.Append(MakeInstance(ballot, index3));

  log_.CommitUntil(index3, ballot);
  execute_thread.join();

  EXPECT_TRUE(IsExecuted(*log_[index1]));
  EXPECT_TRUE(IsExecuted(*log_[index2]));
  EXPECT_TRUE(IsExecuted(*log_[index3]));
  EXPECT_FALSE(log_.IsExecutable());
}

TEST_F(LogTest, AppendCommitUntilExecuteTrimUntil) {
  std::thread execute_thread([this] {
    log_.Execute(&store_);
    log_.Execute(&store_);
    log_.Execute(&store_);
  });

  auto ballot = 0;
  auto index1 = 1;
  log_.Append(MakeInstance(ballot, index1));
  auto index2 = 2;
  log_.Append(MakeInstance(ballot, index2));
  auto index3 = 3;
  log_.Append(MakeInstance(ballot, index3));

  log_.CommitUntil(index3, ballot);
  execute_thread.join();

  log_.TrimUntil(index3);

  EXPECT_EQ(nullptr, log_[index1]);
  EXPECT_EQ(nullptr, log_[index2]);
  EXPECT_EQ(nullptr, log_[index3]);
  EXPECT_EQ(index3, log_.LastExecuted());
  EXPECT_EQ(index3, log_.GlobalLastExecuted());
  EXPECT_FALSE(log_.IsExecutable());
}

TEST_F(LogTest, AppendAtTrimmedIndex) {
  std::thread execute_thread([this] {
    log_.Execute(&store_);
    log_.Execute(&store_);
  });

  auto ballot = 0;
  auto index1 = 1;
  log_.Append(MakeInstance(ballot, index1));
  auto index2 = 2;
  log_.Append(MakeInstance(ballot, index2));

  log_.CommitUntil(index2, ballot);
  execute_thread.join();

  log_.TrimUntil(index2);

  EXPECT_EQ(nullptr, log_[index1]);
  EXPECT_EQ(nullptr, log_[index2]);
  EXPECT_EQ(index2, log_.LastExecuted());
  EXPECT_EQ(index2, log_.GlobalLastExecuted());
  EXPECT_FALSE(log_.IsExecutable());

  log_.Append(MakeInstance(ballot, index1));
  log_.Append(MakeInstance(ballot, index2));
  EXPECT_EQ(nullptr, log_[index1]);
  EXPECT_EQ(nullptr, log_[index2]);
}

TEST_F(LogTest, InstancesForPrepare) {
  std::thread execute_thread([this] {
    log_.Execute(&store_);
    log_.Execute(&store_);
  });

  auto ballot = 0;
  std::vector<Instance> expected;

  expected.emplace_back(MakeInstance(ballot));
  log_.Append(expected.back());
  expected.emplace_back(MakeInstance(ballot));
  log_.Append(expected.back());
  expected.emplace_back(MakeInstance(ballot));
  log_.Append(expected.back());

  EXPECT_EQ(expected, log_.InstancesForPrepare());

  auto index = 2;
  log_.CommitUntil(index, ballot);
  execute_thread.join();
  log_.TrimUntil(index);

  expected.erase(expected.begin(), expected.begin() + index);
  EXPECT_EQ(expected, log_.InstancesForPrepare());
}
