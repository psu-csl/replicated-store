#include <gtest/gtest.h>

#include "log.h"

TEST(LogTest, Constructor) {
  Log log;

  EXPECT_EQ(log.LastExecuted(), 0);
  EXPECT_EQ(log.GlobalLastExecuted(), 0);
  EXPECT_FALSE(log.IsExecutable());
}