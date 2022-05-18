#include <gtest/gtest.h>

#include "log.h"

TEST(LogTest, Constructor) {
  Log log;

  EXPECT_EQ(log.LastExecuted(), 0);
  EXPECT_EQ(log.GlobalLastExecuted(), 0);
  EXPECT_FALSE(log.IsExecutable());
  EXPECT_EQ(nullptr, log[0]);
  EXPECT_EQ(nullptr, log[-1]);
  EXPECT_EQ(nullptr, log[3]);
}

TEST(LogTest, Append) {
  // append two instances at indexes 1 and 2 to the log and ensure they are in
  // the log.
  {
    Log log;
    Command cmd;

    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kInProgress, cmd};
    log.Append(std::move(i1));
    EXPECT_EQ(index, log[index]->index_);

    index = log.AdvanceLastIndex();
    Instance i2{0, index, 0, InstanceState::kInProgress, cmd};
    log.Append(std::move(i2));
    EXPECT_EQ(index, log[index]->index_);
  }
  // append an instance in executed state and ensure the state was updated to
  // committed.
  {
    Log log;
    Command cmd;

    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kExecuted, cmd};
    log.Append(std::move(i1));
    EXPECT_EQ(InstanceState::kCommitted, log[index]->state_);
  }
  // append an instance at a high index and ensure that the last_index_ was
  // updated.
  {
    Log log;
    Command cmd;

    int64_t index = 42;
    Instance i1{0, index, 0, InstanceState::kExecuted, cmd};
    log.Append(std::move(i1));
    EXPECT_EQ(index, log[index]->index_);
    EXPECT_EQ(index + 1, log.AdvanceLastIndex());
  }
  // append an instance at an index containing an instance with a lower ballot
  // number and  ensure that the instance in the log is updated.
  {
    Log log;
    Command cmd1{CommandType::kPut, "", ""};

    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kInProgress, cmd1};
    log.Append(std::move(i1));
    EXPECT_EQ(index, log[index]->index_);
    EXPECT_EQ(CommandType::kPut, log[index]->command_.type_);

    Command cmd2{CommandType::kDel, "", ""};
    Instance i2{1, index, 0, InstanceState::kInProgress, cmd2};
    log.Append(std::move(i2));
    EXPECT_EQ(CommandType::kDel, log[index]->command_.type_);
  }
  // append an instance at an index containing an instance with a higher ballot
  // number and ensure that the instance in the log is not modified.
  {
    Log log;
    Command cmd1{CommandType::kPut, "", ""};

    int64_t index = log.AdvanceLastIndex();
    Instance i1{2, index, 0, InstanceState::kInProgress, cmd1};
    log.Append(std::move(i1));
    EXPECT_EQ(index, log[index]->index_);
    EXPECT_EQ(CommandType::kPut, log[index]->command_.type_);

    Command cmd2{CommandType::kDel, "", ""};
    Instance i2{1, index, 0, InstanceState::kInProgress, cmd2};
    log.Append(std::move(i2));
    EXPECT_EQ(CommandType::kPut, log[index]->command_.type_);
  }
}

TEST(LogDeathTest, Append) {
  // ensure that the assertion fires for case (3) from the design doc.
  {
    Log log;
    Command cmd1;

    // append an entry at index 1 in committed state
    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kCommitted, cmd1};
    log.Append(std::move(i1));

    // append another entry at index 1 with a different command
    Command cmd2{CommandType::kPut, "", ""};
    Instance i2{0, index, 0, InstanceState::kInProgress, cmd2};
    ASSERT_DEATH(log.Append(std::move(i2)), "case 3");
  }
  // same as above, except when the instance already in the log is in executed
  // state.
  {
    Log log;
    Command cmd1;

    // append an entry at index 1 in executed state
    int64_t index = log.AdvanceLastIndex();
    Instance i1{0, index, 0, InstanceState::kExecuted, cmd1};
    log.Append(std::move(i1));

    // append another entry at index 1 with a different command
    Command cmd2{CommandType::kPut, "", ""};
    Instance i2{0, index, 0, InstanceState::kInProgress, cmd2};
    EXPECT_DEATH(log.Append(std::move(i2)), "");
  }
}
