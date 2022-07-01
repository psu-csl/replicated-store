#include <gtest/gtest.h>

#include "memkvstore.h"

std::string const key1 = "foo";
std::string const val1 = "bar";

std::string const key2 = "baz";
std::string const val2 = "quux";

using multipaxos::Command;
using multipaxos::CommandType;
using multipaxos::CommandType::DEL;
using multipaxos::CommandType::GET;
using multipaxos::CommandType::PUT;

TEST(MemKVStoreTest, GetPutDel) {
  MemKVStore store;

  EXPECT_EQ(nullptr, store.Get(key1));

  EXPECT_FALSE(store.Del(key1));

  EXPECT_TRUE(store.Put(key1, val1));
  EXPECT_EQ(val1, *store.Get(key1));

  EXPECT_TRUE(store.Put(key2, val2));
  EXPECT_EQ(val2, *store.Get(key2));

  EXPECT_TRUE(store.Put(key1, val2));
  EXPECT_EQ(val2, *store.Get(key1));
  EXPECT_EQ(val2, *store.Get(key2));

  EXPECT_TRUE(store.Del(key1));
  EXPECT_EQ(nullptr, store.Get(key1));
  EXPECT_EQ(val2, *store.Get(key2));

  EXPECT_TRUE(store.Del(key2));
  EXPECT_EQ(nullptr, store.Get(key1));
  EXPECT_EQ(nullptr, store.Get(key2));
}

Command MakeCommand(CommandType type,
                    const std::string& key,
                    const std::string& value) {
  Command cmd;
  cmd.set_type(type);
  cmd.set_key(key);
  cmd.set_value(value);
  return cmd;
}

TEST(MemKVStoreTest, Execute) {
  MemKVStore store;
  {
    Result r = store.Execute(MakeCommand(GET, key1, ""));
    EXPECT_TRUE(!r.ok_ && *r.value_ == kKeyNotFound);
  }
  {
    Result r = store.Execute(MakeCommand(DEL, key1, ""));
    EXPECT_TRUE(!r.ok_ && *r.value_ == kKeyNotFound);
  }
  {
    Result r1 = store.Execute(MakeCommand(PUT, key1, val1));
    EXPECT_TRUE(r1.ok_ && *r1.value_ == kEmpty);

    Result r2 = store.Execute(MakeCommand(GET, key1, ""));
    EXPECT_TRUE(r2.ok_ && *r2.value_ == val1);
  }
  {
    Result r1 = store.Execute(MakeCommand(PUT, key2, val2));
    EXPECT_TRUE(r1.ok_ && *r1.value_ == kEmpty);

    Result r2 = store.Execute(MakeCommand(GET, key2, ""));
    EXPECT_TRUE(r2.ok_ && *r2.value_ == val2);
  }
  {
    Result r1 = store.Execute(MakeCommand(PUT, key1, val2));
    EXPECT_TRUE(r1.ok_ && *r1.value_ == kEmpty);

    Result r2 = store.Execute(MakeCommand(GET, key1, ""));
    EXPECT_TRUE(r2.ok_ && *r2.value_ == val2);

    Result r3 = store.Execute(MakeCommand(GET, key2, ""));
    EXPECT_TRUE(r3.ok_ && *r3.value_ == val2);
  }
  {
    Result r1 = store.Execute(MakeCommand(DEL, key1, ""));
    EXPECT_TRUE(r1.ok_ && *r1.value_ == kEmpty);

    Result r2 = store.Execute(MakeCommand(GET, key1, ""));
    EXPECT_FALSE(r2.ok_ && *r2.value_ == kKeyNotFound);

    Result r3 = store.Execute(MakeCommand(GET, key2, ""));
    EXPECT_TRUE(r3.ok_ && *r3.value_ == val2);
  }
}
