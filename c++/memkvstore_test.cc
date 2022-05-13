#include <gtest/gtest.h>

#include "memkvstore.h"

std::string const key1 = "foo";
std::string const val1 = "bar";

std::string const key2 = "baz";
std::string const val2 = "quux";

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

TEST(MemKVStoreTest, Execute) {
  MemKVStore store;
  {
    Command get{CommandType::kGet, key1, ""};
    Result r = store.Execute(get);
    EXPECT_TRUE(!r.ok_ && *r.value_ == kKeyNotFound);
  }
  {
    Command del{CommandType::kDel, key1, ""};
    Result r = store.Execute(del);
    EXPECT_TRUE(!r.ok_ && *r.value_ == kKeyNotFound);
  }
  {
    Command put{CommandType::kPut, key1, val1};
    Result r1 = store.Execute(put);
    EXPECT_TRUE(r1.ok_ && *r1.value_ == kEmpty);

    Command get{CommandType::kGet, key1, ""};
    Result r2 = store.Execute(get);
    EXPECT_TRUE(r2.ok_ && *r2.value_ == val1);
  }
  {
    Command put{CommandType::kPut, key2, val2};
    Result r1 = store.Execute(put);
    EXPECT_TRUE(r1.ok_ && *r1.value_ == kEmpty);

    Command get{CommandType::kGet, key2, ""};
    Result r2 = store.Execute(get);
    EXPECT_TRUE(r2.ok_ && *r2.value_ == val2);
  }
  {
    Command put{CommandType::kPut, key1, val2};
    Result r1 = store.Execute(put);
    EXPECT_TRUE(r1.ok_ && *r1.value_ == kEmpty);

    Command get1{CommandType::kGet, key1, ""};
    Result r2 = store.Execute(get1);
    EXPECT_TRUE(r2.ok_ && *r2.value_ == val2);

    Command get2{CommandType::kGet, key2, ""};
    Result r3 = store.Execute(get2);
    EXPECT_TRUE(r3.ok_ && *r3.value_ == val2);
  }
  {
    Command del{CommandType::kDel, key1, ""};
    Result r1 = store.Execute(del);
    EXPECT_TRUE(r1.ok_ && *r1.value_ == kEmpty);

    Command get1{CommandType::kGet, key1, ""};
    Result r2 = store.Execute(get1);
    EXPECT_FALSE(r2.ok_ && *r2.value_ == kKeyNotFound);

    Command get2{CommandType::kGet, key2, ""};
    Result r3 = store.Execute(get2);
    EXPECT_TRUE(r3.ok_ && *r3.value_ == val2);
  }
}
