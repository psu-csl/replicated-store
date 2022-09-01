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
  kvstore::MemKVStore store;

  EXPECT_EQ(std::nullopt, store.Get(key1));

  EXPECT_FALSE(store.Del(key1));

  EXPECT_TRUE(store.Put(key1, val1));
  EXPECT_EQ(val1, *store.Get(key1));

  EXPECT_TRUE(store.Put(key2, val2));
  EXPECT_EQ(val2, *store.Get(key2));

  EXPECT_TRUE(store.Put(key1, val2));
  EXPECT_EQ(val2, *store.Get(key1));
  EXPECT_EQ(val2, *store.Get(key2));

  EXPECT_TRUE(store.Del(key1));
  EXPECT_EQ(std::nullopt, store.Get(key1));
  EXPECT_EQ(val2, *store.Get(key2));

  EXPECT_TRUE(store.Del(key2));
  EXPECT_EQ(std::nullopt, store.Get(key1));
  EXPECT_EQ(std::nullopt, store.Get(key2));
}

Command MakeCommand(CommandType type,
                    std::string const& key,
                    std::string const& value) {
  Command cmd;
  cmd.set_type(type);
  cmd.set_key(key);
  cmd.set_value(value);
  return cmd;
}

TEST(MemKVStoreTest, Execute) {
  auto get_key1 = MakeCommand(GET, key1, "");
  auto get_key2 = MakeCommand(GET, key2, "");
  auto del_key1 = MakeCommand(DEL, key1, "");
  auto put_key1val1 = MakeCommand(PUT, key1, val1);
  auto put_key2val2 = MakeCommand(PUT, key2, val2);
  auto put_key1val2 = MakeCommand(PUT, key1, val2);

  kvstore::MemKVStore store;
  {
    kvstore::KVResult r = kvstore::Execute(get_key1, &store);
    EXPECT_TRUE(!r.ok_ && r.value_ == kvstore::kNotFound);
  }
  {
    kvstore::KVResult r = kvstore::Execute(del_key1, &store);
    EXPECT_TRUE(!r.ok_ && r.value_ == kvstore::kNotFound);
  }
  {
    kvstore::KVResult r1 = kvstore::Execute(put_key1val1, &store);
    EXPECT_TRUE(r1.ok_ && r1.value_ == kvstore::kEmpty);

    kvstore::KVResult r2 = kvstore::Execute(get_key1, &store);
    EXPECT_TRUE(r2.ok_ && r2.value_ == val1);
  }
  {
    kvstore::KVResult r1 = kvstore::Execute(put_key2val2, &store);
    EXPECT_TRUE(r1.ok_ && r1.value_ == kvstore::kEmpty);

    kvstore::KVResult r2 = kvstore::Execute(get_key2, &store);
    EXPECT_TRUE(r2.ok_ && r2.value_ == val2);
  }
  {
    kvstore::KVResult r1 = kvstore::Execute(put_key1val2, &store);
    EXPECT_TRUE(r1.ok_ && r1.value_ == kvstore::kEmpty);

    kvstore::KVResult r2 = kvstore::Execute(get_key1, &store);
    EXPECT_TRUE(r2.ok_ && r2.value_ == val2);

    kvstore::KVResult r3 = kvstore::Execute(get_key2, &store);
    EXPECT_TRUE(r3.ok_ && r3.value_ == val2);
  }
  {
    kvstore::KVResult r1 = kvstore::Execute(del_key1, &store);
    EXPECT_TRUE(r1.ok_ && r1.value_ == kvstore::kEmpty);

    kvstore::KVResult r2 = kvstore::Execute(get_key1, &store);
    EXPECT_FALSE(r2.ok_ && r2.value_ == kvstore::kNotFound);

    kvstore::KVResult r3 = kvstore::Execute(get_key2, &store);
    EXPECT_TRUE(r3.ok_ && r3.value_ == val2);
  }
}
