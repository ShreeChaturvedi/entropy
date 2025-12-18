/**
 * @file hash_index_test.cpp
 * @brief Unit tests for ExtendibleHashTable
 */

#include <gtest/gtest.h>

#include "storage/hash_index.hpp"

namespace entropy {

class HashIndexTest : public ::testing::Test {
protected:
  void SetUp() override {
    hash_index_ = std::make_unique<HashIndex<int32_t, int64_t>>(2);
  }

  std::unique_ptr<HashIndex<int32_t, int64_t>> hash_index_;
};

TEST_F(HashIndexTest, InsertAndFind) {
  ASSERT_TRUE(hash_index_->insert(1, 100).ok());
  ASSERT_TRUE(hash_index_->insert(2, 200).ok());
  ASSERT_TRUE(hash_index_->insert(3, 300).ok());

  EXPECT_EQ(hash_index_->find(1), 100);
  EXPECT_EQ(hash_index_->find(2), 200);
  EXPECT_EQ(hash_index_->find(3), 300);
  EXPECT_EQ(hash_index_->find(4), std::nullopt);
}

TEST_F(HashIndexTest, DuplicateKey) {
  ASSERT_TRUE(hash_index_->insert(1, 100).ok());
  auto status = hash_index_->insert(1, 200);
  EXPECT_FALSE(status.ok()); // Duplicate key
}

TEST_F(HashIndexTest, Remove) {
  ASSERT_TRUE(hash_index_->insert(1, 100).ok());
  ASSERT_TRUE(hash_index_->insert(2, 200).ok());

  EXPECT_TRUE(hash_index_->contains(1));
  ASSERT_TRUE(hash_index_->remove(1).ok());
  EXPECT_FALSE(hash_index_->contains(1));

  auto status = hash_index_->remove(1);
  EXPECT_FALSE(status.ok()); // Already removed
}

TEST_F(HashIndexTest, ManyInserts) {
  // Insert enough entries to trigger bucket splits
  for (int i = 0; i < 1000; ++i) {
    ASSERT_TRUE(hash_index_->insert(i, i * 10).ok());
  }

  // Verify all entries
  for (int i = 0; i < 1000; ++i) {
    auto val = hash_index_->find(i);
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, i * 10);
  }

  EXPECT_EQ(hash_index_->size(), 1000);
}

TEST_F(HashIndexTest, DirectoryGrowth) {
  uint32_t initial_depth = hash_index_->global_depth();

  // Insert many entries to trigger directory growth
  for (int i = 0; i < 500; ++i) {
    ASSERT_TRUE(hash_index_->insert(i, i).ok());
  }

  // Global depth should have increased
  EXPECT_GE(hash_index_->global_depth(), initial_depth);
}

TEST_F(HashIndexTest, StringKeys) {
  HashIndex<std::string, int32_t> string_index(2);

  ASSERT_TRUE(string_index.insert("hello", 1).ok());
  ASSERT_TRUE(string_index.insert("world", 2).ok());
  ASSERT_TRUE(string_index.insert("foo", 3).ok());

  EXPECT_EQ(string_index.find("hello"), 1);
  EXPECT_EQ(string_index.find("world"), 2);
  EXPECT_EQ(string_index.find("foo"), 3);
  EXPECT_EQ(string_index.find("bar"), std::nullopt);
}

TEST_F(HashIndexTest, RIDValues) {
  HashIndex<int32_t, RID> rid_index(2);

  RID rid1(1, 0);
  RID rid2(2, 1);
  RID rid3(3, 2);

  ASSERT_TRUE(rid_index.insert(100, rid1).ok());
  ASSERT_TRUE(rid_index.insert(200, rid2).ok());
  ASSERT_TRUE(rid_index.insert(300, rid3).ok());

  auto found = rid_index.find(100);
  ASSERT_TRUE(found.has_value());
  EXPECT_EQ(found->page_id, rid1.page_id);
  EXPECT_EQ(found->slot_id, rid1.slot_id);
}

} // namespace entropy
