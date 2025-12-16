/**
 * @file b_plus_tree_test.cpp
 * @brief Unit tests for B+ Tree index operations
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <random>
#include <vector>

#include "storage/b_plus_tree.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/disk_manager.hpp"

namespace entropy {
namespace {

// ─────────────────────────────────────────────────────────────────────────────
// Test Fixture
// ─────────────────────────────────────────────────────────────────────────────

class BPlusTreeTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temporary directory for test files
        test_dir_ = std::filesystem::temp_directory_path() / "entropy_bptree_test";
        std::filesystem::create_directories(test_dir_);

        db_file_ = test_dir_ / "test.db";

        // Clean up any existing file
        std::filesystem::remove(db_file_);

        // Create disk manager and buffer pool
        disk_manager_ = std::make_shared<DiskManager>(db_file_.string());
        buffer_pool_ = std::make_shared<BufferPoolManager>(100, disk_manager_);
    }

    void TearDown() override {
        buffer_pool_.reset();
        disk_manager_.reset();
        std::filesystem::remove_all(test_dir_);
    }

    std::filesystem::path test_dir_;
    std::filesystem::path db_file_;
    std::shared_ptr<DiskManager> disk_manager_;
    std::shared_ptr<BufferPoolManager> buffer_pool_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Basic Operations
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(BPlusTreeTest, EmptyTree) {
    BPlusTree tree(buffer_pool_);

    EXPECT_TRUE(tree.is_empty());
    EXPECT_EQ(tree.root_page_id(), INVALID_PAGE_ID);

    auto result = tree.find(42);
    EXPECT_FALSE(result.has_value());
}

TEST_F(BPlusTreeTest, InsertSingle) {
    BPlusTree tree(buffer_pool_);

    RID rid(10, 5);
    auto status = tree.insert(42, rid);
    EXPECT_TRUE(status.ok()) << status.message();

    EXPECT_FALSE(tree.is_empty());
    EXPECT_NE(tree.root_page_id(), INVALID_PAGE_ID);

    auto result = tree.find(42);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->page_id, 10);
    EXPECT_EQ(result->slot_id, 5);
}

TEST_F(BPlusTreeTest, InsertMultiple) {
    BPlusTree tree(buffer_pool_);

    // Insert several values
    EXPECT_TRUE(tree.insert(30, RID(1, 0)).ok());
    EXPECT_TRUE(tree.insert(10, RID(2, 0)).ok());
    EXPECT_TRUE(tree.insert(50, RID(3, 0)).ok());
    EXPECT_TRUE(tree.insert(20, RID(4, 0)).ok());
    EXPECT_TRUE(tree.insert(40, RID(5, 0)).ok());

    // Verify all can be found
    auto r1 = tree.find(10);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->page_id, 2);

    auto r2 = tree.find(20);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->page_id, 4);

    auto r3 = tree.find(30);
    ASSERT_TRUE(r3.has_value());
    EXPECT_EQ(r3->page_id, 1);

    auto r4 = tree.find(40);
    ASSERT_TRUE(r4.has_value());
    EXPECT_EQ(r4->page_id, 5);

    auto r5 = tree.find(50);
    ASSERT_TRUE(r5.has_value());
    EXPECT_EQ(r5->page_id, 3);
}

TEST_F(BPlusTreeTest, InsertDuplicate) {
    BPlusTree tree(buffer_pool_);

    EXPECT_TRUE(tree.insert(42, RID(1, 0)).ok());

    auto status = tree.insert(42, RID(2, 0));
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kAlreadyExists);
}

TEST_F(BPlusTreeTest, FindNonExistent) {
    BPlusTree tree(buffer_pool_);

    (void)tree.insert(10, RID(1, 0));
    (void)tree.insert(30, RID(3, 0));

    auto result = tree.find(20);
    EXPECT_FALSE(result.has_value());

    auto result2 = tree.find(5);
    EXPECT_FALSE(result2.has_value());

    auto result3 = tree.find(100);
    EXPECT_FALSE(result3.has_value());
}

// ─────────────────────────────────────────────────────────────────────────────
// Deletion Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(BPlusTreeTest, RemoveSingle) {
    BPlusTree tree(buffer_pool_);

    (void)tree.insert(42, RID(1, 0));

    auto status = tree.remove(42);
    EXPECT_TRUE(status.ok());

    auto result = tree.find(42);
    EXPECT_FALSE(result.has_value());
}

TEST_F(BPlusTreeTest, RemoveNonExistent) {
    BPlusTree tree(buffer_pool_);

    (void)tree.insert(10, RID(1, 0));

    auto status = tree.remove(20);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kNotFound);
}

TEST_F(BPlusTreeTest, RemoveMultiple) {
    BPlusTree tree(buffer_pool_);

    // Insert values
    for (int i = 1; i <= 10; ++i) {
        (void)tree.insert(i * 10, RID(i, 0));
    }

    // Remove some
    EXPECT_TRUE(tree.remove(30).ok());
    EXPECT_TRUE(tree.remove(70).ok());
    EXPECT_TRUE(tree.remove(10).ok());

    // Verify removed ones are gone
    EXPECT_FALSE(tree.find(30).has_value());
    EXPECT_FALSE(tree.find(70).has_value());
    EXPECT_FALSE(tree.find(10).has_value());

    // Verify others still exist
    EXPECT_TRUE(tree.find(20).has_value());
    EXPECT_TRUE(tree.find(40).has_value());
    EXPECT_TRUE(tree.find(50).has_value());
}

// ─────────────────────────────────────────────────────────────────────────────
// Range Scan Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(BPlusTreeTest, RangeScanBasic) {
    BPlusTree tree(buffer_pool_);

    // Insert values 10, 20, ..., 100
    for (int i = 1; i <= 10; ++i) {
        (void)tree.insert(i * 10, RID(i, 0));
    }

    // Scan range [25, 75]
    auto results = tree.range_scan(25, 75);

    // Should get 30, 40, 50, 60, 70
    ASSERT_EQ(results.size(), 5);
    EXPECT_EQ(results[0].first, 30);
    EXPECT_EQ(results[1].first, 40);
    EXPECT_EQ(results[2].first, 50);
    EXPECT_EQ(results[3].first, 60);
    EXPECT_EQ(results[4].first, 70);
}

TEST_F(BPlusTreeTest, RangeScanExactBoundaries) {
    BPlusTree tree(buffer_pool_);

    for (int i = 1; i <= 10; ++i) {
        (void)tree.insert(i * 10, RID(i, 0));
    }

    // Exact boundaries [30, 60]
    auto results = tree.range_scan(30, 60);

    ASSERT_EQ(results.size(), 4);
    EXPECT_EQ(results[0].first, 30);
    EXPECT_EQ(results[1].first, 40);
    EXPECT_EQ(results[2].first, 50);
    EXPECT_EQ(results[3].first, 60);
}

TEST_F(BPlusTreeTest, RangeScanEmpty) {
    BPlusTree tree(buffer_pool_);

    for (int i = 1; i <= 10; ++i) {
        (void)tree.insert(i * 10, RID(i, 0));
    }

    // No values in range [15, 19]
    auto results = tree.range_scan(15, 19);
    EXPECT_TRUE(results.empty());
}

TEST_F(BPlusTreeTest, RangeScanAll) {
    BPlusTree tree(buffer_pool_);

    for (int i = 1; i <= 10; ++i) {
        (void)tree.insert(i * 10, RID(i, 0));
    }

    auto results = tree.range_scan(0, 1000);
    EXPECT_EQ(results.size(), 10);
}

TEST_F(BPlusTreeTest, RangeScanEmptyTree) {
    BPlusTree tree(buffer_pool_);

    auto results = tree.range_scan(0, 100);
    EXPECT_TRUE(results.empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// Iterator Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(BPlusTreeTest, IteratorBasic) {
    BPlusTree tree(buffer_pool_);

    for (int i = 1; i <= 5; ++i) {
        (void)tree.insert(i * 10, RID(i, 0));
    }

    std::vector<int64_t> keys;
    for (auto it = tree.begin(); it != tree.end(); ++it) {
        keys.push_back(it.key());
    }

    ASSERT_EQ(keys.size(), 5);
    EXPECT_EQ(keys[0], 10);
    EXPECT_EQ(keys[1], 20);
    EXPECT_EQ(keys[2], 30);
    EXPECT_EQ(keys[3], 40);
    EXPECT_EQ(keys[4], 50);
}

TEST_F(BPlusTreeTest, IteratorEmpty) {
    BPlusTree tree(buffer_pool_);

    auto it = tree.begin();
    EXPECT_TRUE(it.is_end());
    EXPECT_EQ(it, tree.end());
}

TEST_F(BPlusTreeTest, LowerBound) {
    BPlusTree tree(buffer_pool_);

    for (int i = 1; i <= 10; ++i) {
        (void)tree.insert(i * 10, RID(i, 0));
    }

    // Exact match
    auto it1 = tree.lower_bound(50);
    ASSERT_FALSE(it1.is_end());
    EXPECT_EQ(it1.key(), 50);

    // No exact match - should find next
    auto it2 = tree.lower_bound(55);
    ASSERT_FALSE(it2.is_end());
    EXPECT_EQ(it2.key(), 60);

    // Before all
    auto it3 = tree.lower_bound(5);
    ASSERT_FALSE(it3.is_end());
    EXPECT_EQ(it3.key(), 10);

    // After all
    auto it4 = tree.lower_bound(200);
    EXPECT_TRUE(it4.is_end());
}

TEST_F(BPlusTreeTest, IteratorDereference) {
    BPlusTree tree(buffer_pool_);

    (void)tree.insert(42, RID(10, 5));

    auto it = tree.begin();
    auto [key, value] = *it;

    EXPECT_EQ(key, 42);
    EXPECT_EQ(value.page_id, 10);
    EXPECT_EQ(value.slot_id, 5);
}

// ─────────────────────────────────────────────────────────────────────────────
// Split Tests (Larger Insertions)
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(BPlusTreeTest, InsertCausesSplit) {
    BPlusTree tree(buffer_pool_);

    // Insert enough values to cause at least one split
    // With ~250 entries per leaf, we need a lot of inserts
    for (int i = 0; i < 500; ++i) {
        auto status = tree.insert(i, RID(i, 0));
        EXPECT_TRUE(status.ok()) << "Failed at i=" << i << ": " << status.message();
    }

    // Verify all values can be found
    for (int i = 0; i < 500; ++i) {
        auto result = tree.find(i);
        ASSERT_TRUE(result.has_value()) << "Key " << i << " not found";
        EXPECT_EQ(result->page_id, static_cast<uint32_t>(i));
    }
}

TEST_F(BPlusTreeTest, InsertReverseOrder) {
    BPlusTree tree(buffer_pool_);

    // Insert in reverse order
    for (int i = 299; i >= 0; --i) {
        auto status = tree.insert(i, RID(i, 0));
        EXPECT_TRUE(status.ok()) << "Failed at i=" << i;
    }

    // Verify all values
    for (int i = 0; i < 300; ++i) {
        auto result = tree.find(i);
        ASSERT_TRUE(result.has_value()) << "Key " << i << " not found";
    }

    // Verify iteration order is correct
    auto it = tree.begin();
    for (int i = 0; i < 300; ++i) {
        ASSERT_FALSE(it.is_end()) << "Iterator ended early at i=" << i;
        EXPECT_EQ(it.key(), i);
        ++it;
    }
    EXPECT_TRUE(it.is_end());
}

TEST_F(BPlusTreeTest, InsertRandomOrder) {
    BPlusTree tree(buffer_pool_);

    // Generate random order
    std::vector<int> values;
    for (int i = 0; i < 300; ++i) {
        values.push_back(i);
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(values.begin(), values.end(), gen);

    // Insert in random order
    for (int v : values) {
        auto status = tree.insert(v, RID(v, 0));
        EXPECT_TRUE(status.ok()) << "Failed inserting " << v;
    }

    // Verify all can be found
    for (int i = 0; i < 300; ++i) {
        auto result = tree.find(i);
        ASSERT_TRUE(result.has_value()) << "Key " << i << " not found";
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(BPlusTreeTest, LargeTreeOperations) {
    BPlusTree tree(buffer_pool_);

    const int N = 1000;

    // Insert N values
    for (int i = 0; i < N; ++i) {
        EXPECT_TRUE(tree.insert(i * 2, RID(i, 0)).ok());
    }

    // Find all
    for (int i = 0; i < N; ++i) {
        auto result = tree.find(i * 2);
        ASSERT_TRUE(result.has_value());
    }

    // Delete every other one
    for (int i = 0; i < N; i += 2) {
        EXPECT_TRUE(tree.remove(i * 2).ok());
    }

    // Verify deletions
    for (int i = 0; i < N; ++i) {
        auto result = tree.find(i * 2);
        if (i % 2 == 0) {
            EXPECT_FALSE(result.has_value()) << "Key " << (i * 2) << " should be deleted";
        } else {
            EXPECT_TRUE(result.has_value()) << "Key " << (i * 2) << " should exist";
        }
    }
}

TEST_F(BPlusTreeTest, MixedOperations) {
    BPlusTree tree(buffer_pool_);

    // Insert some values
    for (int i = 0; i < 100; ++i) {
        (void)tree.insert(i, RID(i, 0));
    }

    // Delete some, insert some more
    for (int i = 0; i < 50; ++i) {
        (void)tree.remove(i);
        (void)tree.insert(i + 100, RID(i + 100, 0));
    }

    // Verify state
    for (int i = 0; i < 50; ++i) {
        EXPECT_FALSE(tree.find(i).has_value());
    }
    for (int i = 50; i < 150; ++i) {
        EXPECT_TRUE(tree.find(i).has_value()) << "Key " << i << " not found";
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge Cases
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(BPlusTreeTest, NegativeKeys) {
    BPlusTree tree(buffer_pool_);

    (void)tree.insert(-50, RID(1, 0));
    (void)tree.insert(-25, RID(2, 0));
    (void)tree.insert(0, RID(3, 0));
    (void)tree.insert(25, RID(4, 0));
    (void)tree.insert(50, RID(5, 0));

    EXPECT_TRUE(tree.find(-50).has_value());
    EXPECT_TRUE(tree.find(-25).has_value());
    EXPECT_TRUE(tree.find(0).has_value());
    EXPECT_TRUE(tree.find(25).has_value());
    EXPECT_TRUE(tree.find(50).has_value());

    // Range scan with negative keys
    auto results = tree.range_scan(-30, 30);
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0].first, -25);
    EXPECT_EQ(results[1].first, 0);
    EXPECT_EQ(results[2].first, 25);
}

TEST_F(BPlusTreeTest, DeleteAllReinsert) {
    BPlusTree tree(buffer_pool_);

    // Insert values
    for (int i = 0; i < 50; ++i) {
        (void)tree.insert(i, RID(i, 0));
    }

    // Delete all
    for (int i = 0; i < 50; ++i) {
        EXPECT_TRUE(tree.remove(i).ok());
    }

    // Tree should be empty
    EXPECT_TRUE(tree.is_empty());

    // Reinsert
    for (int i = 0; i < 50; ++i) {
        EXPECT_TRUE(tree.insert(i, RID(i + 100, 0)).ok());
    }

    // Verify new values
    for (int i = 0; i < 50; ++i) {
        auto result = tree.find(i);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->page_id, static_cast<uint32_t>(i + 100));
    }
}

TEST_F(BPlusTreeTest, SingleElementOperations) {
    BPlusTree tree(buffer_pool_);

    // Insert single
    (void)tree.insert(42, RID(1, 0));
    EXPECT_FALSE(tree.is_empty());

    // Iterate single
    auto it = tree.begin();
    EXPECT_EQ(it.key(), 42);
    ++it;
    EXPECT_TRUE(it.is_end());

    // Delete single
    (void)tree.remove(42);
    EXPECT_TRUE(tree.is_empty());

    // Iterate empty
    it = tree.begin();
    EXPECT_TRUE(it.is_end());
}

// ─────────────────────────────────────────────────────────────────────────────
// Persistence Test (using existing root)
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(BPlusTreeTest, ReuseExistingRoot) {
    page_id_t saved_root;

    {
        BPlusTree tree(buffer_pool_);

        for (int i = 0; i < 100; ++i) {
            (void)tree.insert(i, RID(i, 0));
        }

        saved_root = tree.root_page_id();
        EXPECT_NE(saved_root, INVALID_PAGE_ID);
    }

    // Create new tree with existing root
    BPlusTree tree2(buffer_pool_, saved_root);

    EXPECT_EQ(tree2.root_page_id(), saved_root);
    EXPECT_FALSE(tree2.is_empty());

    // Verify data is still there
    for (int i = 0; i < 100; ++i) {
        auto result = tree2.find(i);
        ASSERT_TRUE(result.has_value()) << "Key " << i << " not found after reopen";
    }
}

}  // namespace
}  // namespace entropy
