#include <gtest/gtest.h>
#include <atomic>
#include <thread>

#include "instance.h"
#include "log.h"
#include "memkvstore.h"

class LogTest : public testing::Test {
 protected:
  Instance MakeInstance(int64_t ballot) {
    return Instance{ballot, log_.AdvanceLastIndex(), 0,
                    InstanceState::kInProgress, Command{}};
  }
  Instance MakeInstance(int64_t ballot, int64_t index) {
    return Instance{ballot, index, 0, InstanceState::kInProgress, Command{}};
  }
  Instance MakeInstance(int64_t ballot, InstanceState state) {
    return Instance{ballot, log_.AdvanceLastIndex(), 0, state, Command{}};
  }
  Instance MakeInstance(int64_t ballot, int64_t index, CommandType type) {
    return Instance{ballot, index, 0, InstanceState::kInProgress,
                    Command{type, "", ""}};
  }
  Instance MakeInstance(int64_t ballot,
                        int64_t index,
                        InstanceState state,
                        CommandType type) {
    return Instance{ballot, index, 0, state, Command{type, "", ""}};
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

TEST_F(LogTest, Append) {
  log_.Append(MakeInstance(0));
  log_.Append(MakeInstance(0));
  EXPECT_EQ(1, log_[1]->index_);
  EXPECT_EQ(2, log_[2]->index_);
}

TEST_F(LogTest, AppendExecuted) {
  log_.Append(MakeInstance(0, InstanceState::kExecuted));
  EXPECT_TRUE(log_[1]->IsCommitted());
}

TEST_F(LogTest, AppendWithGap) {
  auto index = 42;
  log_.Append(MakeInstance(0, index));
  EXPECT_EQ(index, log_[index]->index_);
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
  log_.Append(MakeInstance(lo_ballot, index, CommandType::kPut));
  log_.Append(MakeInstance(hi_ballot, index, CommandType::kDel));
  EXPECT_EQ(CommandType::kDel, log_[index]->command_.type_);
}

TEST_F(LogTest, AppendLowBallotNoEffect) {
  auto index = 1, lo_ballot = 0, hi_ballot = 1;
  log_.Append(MakeInstance(hi_ballot, index, CommandType::kPut));
  log_.Append(MakeInstance(lo_ballot, index, CommandType::kDel));
  EXPECT_EQ(CommandType::kPut, log_[index]->command_.type_);
}

TEST_F(LogDeathTest, AppendCase3Committed) {
  auto index = 1;
  auto inst1 =
      MakeInstance(0, index, InstanceState::kCommitted, CommandType::kPut);
  auto inst2 =
      MakeInstance(0, index, InstanceState::kInProgress, CommandType::kDel);
  log_.Append(std::move(inst1));
  EXPECT_DEATH(log_.Append(std::move(inst2)), "Append case 3");
}

TEST_F(LogDeathTest, AppendCase3Executed) {
  auto index = 1;
  auto inst1 =
      MakeInstance(0, index, InstanceState::kExecuted, CommandType::kPut);
  auto inst2 =
      MakeInstance(0, index, InstanceState::kInProgress, CommandType::kDel);
  log_.Append(std::move(inst1));
  EXPECT_DEATH(log_.Append(std::move(inst2)), "Append case 3");
}

TEST_F(LogDeathTest, AppendCase4) {
  auto index = 1;
  auto inst1 =
      MakeInstance(0, index, InstanceState::kInProgress, CommandType::kPut);
  auto inst2 =
      MakeInstance(0, index, InstanceState::kInProgress, CommandType::kDel);
  log_.Append(std::move(inst1));
  EXPECT_DEATH(log_.Append(std::move(inst2)), "Append case 4");
}

TEST_F(LogTest, Commit) {
  auto index1 = 1;
  log_.Append(MakeInstance(0, index1));
  auto index2 = 2;
  log_.Append(MakeInstance(0, index2));

  EXPECT_TRUE(log_[index1]->IsInProgress());
  EXPECT_TRUE(log_[index2]->IsInProgress());
  EXPECT_FALSE(log_.IsExecutable());

  log_.Commit(index2);

  EXPECT_TRUE(log_[index1]->IsInProgress());
  EXPECT_TRUE(log_[index2]->IsCommitted());
  EXPECT_FALSE(log_.IsExecutable());

  log_.Commit(index1);

  EXPECT_TRUE(log_[index1]->IsCommitted());
  EXPECT_TRUE(log_[index2]->IsCommitted());
  EXPECT_TRUE(log_.IsExecutable());
}

TEST_F(LogTest, CommitBeforeAppend) {
  auto index = 1;
  std::thread commit_thread([this, index] { log_.Commit(index); });
  std::this_thread::yield();
  log_.Append(MakeInstance(0));
  commit_thread.join();
  EXPECT_TRUE(log_[index]->IsCommitted());
}

TEST_F(LogTest, AppendCommitExecute) {
  auto index = 1;
  std::atomic<bool> done = false;

  std::thread execute_thread([this, &done] {
    while (!done)
      log_.Execute(&store_);
  });

  log_.Append(MakeInstance(0, index));
  done = true;
  log_.Commit(index);
  execute_thread.join();

  EXPECT_TRUE(log_[index]->IsExecuted());
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

  EXPECT_TRUE(log_[index1]->IsExecuted());
  EXPECT_TRUE(log_[index2]->IsExecuted());
  EXPECT_TRUE(log_[index3]->IsExecuted());
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

  EXPECT_TRUE(log_[index1]->IsCommitted());
  EXPECT_TRUE(log_[index2]->IsCommitted());
  EXPECT_FALSE(log_[index3]->IsCommitted());
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

  EXPECT_FALSE(log_[index1]->IsCommitted());
  EXPECT_FALSE(log_[index2]->IsCommitted());
  EXPECT_FALSE(log_[index3]->IsCommitted());
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

  EXPECT_TRUE(log_[index1]->IsCommitted());
  EXPECT_FALSE(log_[index3]->IsCommitted());
  EXPECT_FALSE(log_[index4]->IsCommitted());
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

  EXPECT_TRUE(log_[index1]->IsExecuted());
  EXPECT_TRUE(log_[index2]->IsExecuted());
  EXPECT_TRUE(log_[index3]->IsExecuted());
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
