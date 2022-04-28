#include <gtest/gtest.h>

#include "memkvstore.h"

TEST(MemKVStoreTest, GetPutDel) {
  MemKVStore store;

  std::string const key1 = "foo";
  std::string const val1 = "bar";

  std::string const key2 = "baz";
  std::string const val2 = "quux";

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
