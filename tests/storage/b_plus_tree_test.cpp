/**
 * @file b_plus_tree_test.cpp
 * @brief Unit tests for B+ Tree index operations
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "storage/b_plus_tree.hpp"
#include "storage/b_plus_tree_page.hpp"
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
        disk_manager_ = std::make_shared<FileDiskManager>(db_file_.string());
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
// Delete-path correctness (issue #3): left-merge reclaim + internal underflow
// ─────────────────────────────────────────────────────────────────────────────

namespace {

/// Walk every reachable node and assert non-root nodes meet min fill factor.
void AssertNoUnderflow(BPlusTree& tree) {
    page_id_t root = tree.root_page_id();
    if (root == INVALID_PAGE_ID) {
        return;
    }

    auto bpm = tree.buffer_pool();
    std::vector<page_id_t> queue;
    queue.push_back(root);

    while (!queue.empty()) {
        page_id_t pid = queue.back();
        queue.pop_back();

        Page* page = bpm->fetch_page(pid);
        ASSERT_NE(page, nullptr) << "Failed to fetch page " << pid;
        BPTreePage node(page);

        if (!node.is_root()) {
            EXPECT_FALSE(node.is_underflow())
                << "Node " << pid << " underflows with " << node.num_keys()
                << " keys (min " << node.min_size() << ")";
        }

        if (!node.is_leaf()) {
            BPTreeInternalPage internal(page);
            for (uint32_t i = 0; i <= internal.num_keys(); ++i) {
                page_id_t child = internal.child_at(i);
                ASSERT_NE(child, INVALID_PAGE_ID);
                queue.push_back(child);

                Page* child_page = bpm->fetch_page(child);
                ASSERT_NE(child_page, nullptr);
                BPTreePage child_node(child_page);
                EXPECT_EQ(child_node.parent_page_id(), pid)
                    << "Child " << child << " has wrong parent";
                bpm->unpin_page(child, false);
            }
        }

        bpm->unpin_page(pid, false);
    }
}

}  // namespace

TEST_F(BPlusTreeTest, LeftMergeReclaimsMergedLeafPage) {
    // Small fanout so deletes reliably force leaf merges.
    // Deleting high→low hits the rightmost leaf, which only has a left sibling
    // and therefore takes the left-merge path that previously leaked the page.
    constexpr uint32_t kLeafMax = 4;
    constexpr uint32_t kInternalMax = 4;
    BPlusTree tree(buffer_pool_, INVALID_PAGE_ID, kLeafMax, kInternalMax);

    const size_t free_at_start = buffer_pool_->free_list_size();
    const int N = 24;

    for (int i = 0; i < N; ++i) {
        ASSERT_TRUE(tree.insert(i, RID(i, 0)).ok());
    }

    const size_t free_after_insert = buffer_pool_->free_list_size();
    ASSERT_LT(free_after_insert, free_at_start);

    // Delete high→low to force rightmost-leaf left-merges.
    for (int i = N - 1; i >= 0; --i) {
        ASSERT_TRUE(tree.remove(i).ok()) << "Failed to remove " << i;
    }

    EXPECT_TRUE(tree.is_empty());
    // Every page allocated for the tree must be back on the free list.
    EXPECT_EQ(buffer_pool_->free_list_size(), free_at_start)
        << "Left-merge (or root collapse) failed to reclaim pages";
}

TEST_F(BPlusTreeTest, InternalUnderflowRebalancesAfterDeletes) {
    // Tiny fanout builds a multi-level tree quickly; sustained deletes must
    // rebalance/merge internal nodes (not leave them underfull).
    constexpr uint32_t kLeafMax = 4;
    constexpr uint32_t kInternalMax = 4;
    BPlusTree tree(buffer_pool_, INVALID_PAGE_ID, kLeafMax, kInternalMax);

    const int N = 80;
    for (int i = 0; i < N; ++i) {
        ASSERT_TRUE(tree.insert(i, RID(i, 0)).ok());
    }

    // Delete most keys, leaving a sparse but non-empty tree.
    for (int i = 0; i < N; ++i) {
        if (i % 3 != 0) {
            ASSERT_TRUE(tree.remove(i).ok()) << "Failed to remove " << i;
        }
    }

    // Surviving keys must still be findable.
    for (int i = 0; i < N; i += 3) {
        auto result = tree.find(i);
        ASSERT_TRUE(result.has_value()) << "Key " << i << " missing after rebalance";
        EXPECT_EQ(result->page_id, static_cast<page_id_t>(i));
    }
    for (int i = 0; i < N; ++i) {
        if (i % 3 != 0) {
            EXPECT_FALSE(tree.find(i).has_value());
        }
    }

    AssertNoUnderflow(tree);

    // Finish deleting; pages should fully reclaim.
    const size_t free_before_final = buffer_pool_->free_list_size();
    for (int i = 0; i < N; i += 3) {
        ASSERT_TRUE(tree.remove(i).ok());
    }
    EXPECT_TRUE(tree.is_empty());
    EXPECT_GT(buffer_pool_->free_list_size(), free_before_final);
    EXPECT_EQ(buffer_pool_->free_list_size(),
              buffer_pool_->pool_size());
}

TEST_F(BPlusTreeTest, RightmostLeafLeftMergePreservesRangeScan) {
    constexpr uint32_t kLeafMax = 4;
    constexpr uint32_t kInternalMax = 4;
    BPlusTree tree(buffer_pool_, INVALID_PAGE_ID, kLeafMax, kInternalMax);

    for (int i = 0; i < 16; ++i) {
        ASSERT_TRUE(tree.insert(i, RID(i, 0)).ok());
    }

    // Remove keys from the high end until several left-merges occur.
    for (int i = 15; i >= 8; --i) {
        ASSERT_TRUE(tree.remove(i).ok());
    }

    auto results = tree.range_scan(0, 100);
    ASSERT_EQ(results.size(), 8u);
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(results[static_cast<size_t>(i)].first, i);
    }
    AssertNoUnderflow(tree);
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

// ─────────────────────────────────────────────────────────────────────────────
// Concurrency tests (issue #5): latch crabbing under concurrent readers/writers.
//
// Structural writers are serialized internally, so these exercise the
// reader-vs-writer crabbing protocol: point reads, range scans, inserts, and
// deletes running together while splits and merges reshape the tree. A latch
// ordering bug would surface as a hang (caught by the test's join + the ctest
// timeout); a missing latch would surface as a torn/wrong value or an
// inconsistent scan (checked below). Built to run clean under ThreadSanitizer.
// ─────────────────────────────────────────────────────────────────────────────

class ConcurrentBPlusTreeTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Unique temp dir per test: high-resolution clock + a process-lifetime
        // counter + a random draw make collisions effectively impossible even
        // under parallel test execution.
        static std::atomic<uint64_t> counter{0};
        const auto stamp =
            std::chrono::steady_clock::now().time_since_epoch().count();
        std::random_device rd;
        const std::string unique = std::to_string(stamp) + "_" +
                                   std::to_string(counter.fetch_add(1)) + "_" +
                                   std::to_string(rd());
        test_dir_ = std::filesystem::temp_directory_path() /
                    ("entropy_bptree_conc_" + unique);
        std::filesystem::create_directories(test_dir_);
        db_file_ = test_dir_ / "test.db";

        disk_manager_ = std::make_shared<FileDiskManager>(db_file_.string());
        // Generous pool so a burst of concurrent pins never exhausts frames.
        buffer_pool_ = std::make_shared<BufferPoolManager>(2048, disk_manager_);
    }

    void TearDown() override {
        // Close handles before removing the directory (Windows CI).
        buffer_pool_.reset();
        disk_manager_.reset();
        std::error_code ec;
        std::filesystem::remove_all(test_dir_, ec);
    }

    static RID rid_for(int64_t key) {
        return RID(static_cast<page_id_t>(key), 0);
    }

    std::filesystem::path test_dir_;
    std::filesystem::path db_file_;
    std::shared_ptr<DiskManager> disk_manager_;
    std::shared_ptr<BufferPoolManager> buffer_pool_;
};

// Many threads inserting disjoint key blocks concurrently: every key must end up
// present exactly once with its correct value.
TEST_F(ConcurrentBPlusTreeTest, ConcurrentDisjointInsertsAreAllVisible) {
    BPlusTree tree(buffer_pool_);
    constexpr int kThreads = 8;
    constexpr int kPerThread = 1000;

    std::atomic<int> insert_fail{0};
    std::atomic<bool> go{false};
    const auto start = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            while (!go.load(std::memory_order_acquire)) {
            }
            for (int i = 0; i < kPerThread; ++i) {
                int64_t key = static_cast<int64_t>(t) * kPerThread + i;
                if (!tree.insert(key, rid_for(key)).ok()) {
                    insert_fail.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    go.store(true, std::memory_order_release);
    for (auto& th : threads) th.join();

    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(
        std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), 60);
    EXPECT_EQ(insert_fail.load(), 0);

    const int total = kThreads * kPerThread;
    for (int64_t key = 0; key < total; ++key) {
        auto v = tree.find(key);
        ASSERT_TRUE(v.has_value()) << "missing key " << key;
        EXPECT_EQ(v->page_id, static_cast<page_id_t>(key));
    }
    auto scan = tree.range_scan(0, total - 1);
    ASSERT_EQ(scan.size(), static_cast<size_t>(total));
    for (size_t i = 0; i < scan.size(); ++i) {
        EXPECT_EQ(scan[i].first, static_cast<int64_t>(i));
    }
}

// Inserts, deletes, point reads, and range scans all running together. Reads
// must never observe a torn/wrong value or an out-of-order scan, and the final
// contents must match the deterministic expected set.
TEST_F(ConcurrentBPlusTreeTest, ConcurrentMixedInsertDeleteRead) {
    BPlusTree tree(buffer_pool_);
    constexpr int64_t P = 2000;        // pre-populate [0, P)
    constexpr int64_t D = 800;         // delete [0, D)
    constexpr int64_t kExtra = 2000;   // insert [P, P + kExtra)
    for (int64_t k = 0; k < P; ++k) {
        ASSERT_TRUE(tree.insert(k, rid_for(k)).ok());
    }

    std::atomic<int> insert_fail{0}, delete_fail{0}, read_bad{0};
    std::atomic<bool> go{false};
    auto wait = [&] {
        while (!go.load(std::memory_order_acquire)) {
        }
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < 2; ++t) {  // inserters split [P, P + kExtra)
        threads.emplace_back([&, t] {
            wait();
            for (int64_t k = P + t; k < P + kExtra; k += 2) {
                if (!tree.insert(k, rid_for(k)).ok()) {
                    insert_fail.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (int t = 0; t < 2; ++t) {  // deleters split [0, D)
        threads.emplace_back([&, t] {
            wait();
            for (int64_t k = t; k < D; k += 2) {
                if (!tree.remove(k).ok()) {
                    delete_fail.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (int t = 0; t < 4; ++t) {  // readers: point reads + range scans
        threads.emplace_back([&, t] {
            wait();
            std::mt19937 gen(1234u + static_cast<unsigned>(t));
            std::uniform_int_distribution<int64_t> dist(0, P + kExtra - 1);
            for (int i = 0; i < 3000; ++i) {
                int64_t k = dist(gen);
                auto v = tree.find(k);
                if (v.has_value() && v->page_id != static_cast<page_id_t>(k)) {
                    read_bad.fetch_add(1, std::memory_order_relaxed);
                }
                if ((i & 63) == 0) {
                    auto scan = tree.range_scan(k, k + 40);
                    int64_t prev = -1;
                    for (auto& [key, val] : scan) {
                        if (key <= prev) {
                            read_bad.fetch_add(1, std::memory_order_relaxed);
                        }
                        if (val.page_id != static_cast<page_id_t>(key)) {
                            read_bad.fetch_add(1, std::memory_order_relaxed);
                        }
                        prev = key;
                    }
                }
            }
        });
    }

    const auto start = std::chrono::steady_clock::now();
    go.store(true, std::memory_order_release);
    for (auto& th : threads) th.join();
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(
        std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), 60);

    EXPECT_EQ(insert_fail.load(), 0);
    EXPECT_EQ(delete_fail.load(), 0);
    EXPECT_EQ(read_bad.load(), 0);

    // Final expected set: [D, P) + [P, P + kExtra) = [D, P + kExtra).
    auto scan = tree.range_scan(D, P + kExtra - 1);
    ASSERT_EQ(scan.size(), static_cast<size_t>(P + kExtra - D));
    for (size_t i = 0; i < scan.size(); ++i) {
        int64_t expected = D + static_cast<int64_t>(i);
        EXPECT_EQ(scan[i].first, expected);
        EXPECT_EQ(scan[i].second.page_id, static_cast<page_id_t>(expected));
    }
    EXPECT_FALSE(tree.find(0).has_value());
    EXPECT_FALSE(tree.find(D - 1).has_value());
    EXPECT_TRUE(tree.find(D).has_value());
}

// Tiny fanout so inserts/deletes constantly split, merge, and borrow. Churners
// hammer disjoint high blocks while scanners read a stable low block that they
// must always see in full -- exercising the crabbing protocol (and would hang on
// a latch-order deadlock) while proving reads stay correct through structural
// churn of shared ancestors.
TEST_F(ConcurrentBPlusTreeTest, ConcurrentSmallFanoutChurn) {
    constexpr uint32_t kLeafMax = 4;
    constexpr uint32_t kInternalMax = 4;
    BPlusTree tree(buffer_pool_, INVALID_PAGE_ID, kLeafMax, kInternalMax);

    constexpr int64_t P = 1200;  // stable low block [0, P)
    for (int64_t k = 0; k < P; ++k) {
        ASSERT_TRUE(tree.insert(k, rid_for(k)).ok());
    }

    constexpr int kChurners = 4;
    constexpr int64_t kBlock = 300;
    constexpr int kRounds = 20;

    std::atomic<int> bad{0};
    std::atomic<bool> go{false};
    auto wait = [&] {
        while (!go.load(std::memory_order_acquire)) {
        }
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < kChurners; ++t) {
        threads.emplace_back([&, t] {
            wait();
            const int64_t base = P + static_cast<int64_t>(t) * kBlock;
            for (int round = 0; round < kRounds; ++round) {
                for (int64_t j = 0; j < kBlock; ++j) {
                    if (!tree.insert(base + j, rid_for(base + j)).ok()) {
                        bad.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                for (int64_t j = 0; j < kBlock; ++j) {
                    if (!tree.remove(base + j).ok()) {
                        bad.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });
    }
    for (int t = 0; t < 2; ++t) {  // scanners of the stable block
        threads.emplace_back([&] {
            wait();
            for (int i = 0; i < 50; ++i) {
                auto scan = tree.range_scan(0, P - 1);
                if (scan.size() != static_cast<size_t>(P)) {
                    bad.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                int64_t prev = -1;
                for (auto& [key, val] : scan) {
                    if (key != prev + 1) {
                        bad.fetch_add(1, std::memory_order_relaxed);
                    }
                    if (val.page_id != static_cast<page_id_t>(key)) {
                        bad.fetch_add(1, std::memory_order_relaxed);
                    }
                    prev = key;
                }
            }
        });
    }

    const auto start = std::chrono::steady_clock::now();
    go.store(true, std::memory_order_release);
    for (auto& th : threads) th.join();
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(
        std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), 120);
    EXPECT_EQ(bad.load(), 0);

    // Stable block fully intact; every churn key removed.
    auto scan = tree.range_scan(0, P - 1);
    ASSERT_EQ(scan.size(), static_cast<size_t>(P));
    for (size_t i = 0; i < scan.size(); ++i) {
        EXPECT_EQ(scan[i].first, static_cast<int64_t>(i));
    }
    for (int64_t k = P; k < P + kChurners * kBlock; ++k) {
        EXPECT_FALSE(tree.find(k).has_value()) << "leftover churn key " << k;
    }
}

}  // namespace
}  // namespace entropy
