/**
 * @file b_plus_tree_page_test.cpp
 * @brief Unit tests for B+ Tree page structures
 */

#include <gtest/gtest.h>

#include <memory>

#include "storage/b_plus_tree_page.hpp"
#include "storage/page.hpp"

namespace entropy {
namespace {

// ─────────────────────────────────────────────────────────────────────────────
// Test Fixture
// ─────────────────────────────────────────────────────────────────────────────

class BPTreePageTest : public ::testing::Test {
protected:
    void SetUp() override {
        page_ = std::make_unique<Page>();
        page_->set_page_id(1);
    }

    std::unique_ptr<Page> page_;
};

// ─────────────────────────────────────────────────────────────────────────────
// BPTreePage Base Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(BPTreePageTest, InitializeLeaf) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(100);

    EXPECT_TRUE(leaf.is_leaf());
    EXPECT_FALSE(leaf.is_internal());
    EXPECT_TRUE(leaf.is_root());  // No parent
    EXPECT_TRUE(leaf.is_empty());
    EXPECT_FALSE(leaf.is_full());
    EXPECT_EQ(leaf.num_keys(), 0);
    EXPECT_EQ(leaf.max_size(), 100);
    EXPECT_EQ(leaf.parent_page_id(), INVALID_PAGE_ID);
}

TEST_F(BPTreePageTest, InitializeInternal) {
    BPTreeInternalPage internal(page_.get());
    internal.init(100);

    EXPECT_FALSE(internal.is_leaf());
    EXPECT_TRUE(internal.is_internal());
    EXPECT_TRUE(internal.is_root());
    EXPECT_TRUE(internal.is_empty());
    EXPECT_EQ(internal.num_keys(), 0);
    EXPECT_EQ(internal.max_size(), 100);
}

TEST_F(BPTreePageTest, ParentPageId) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(100);

    EXPECT_TRUE(leaf.is_root());

    leaf.set_parent_page_id(5);
    EXPECT_EQ(leaf.parent_page_id(), 5);
    EXPECT_FALSE(leaf.is_root());
}

TEST_F(BPTreePageTest, ComputeLeafMaxSize) {
    uint32_t max_size = BPTreeLeafPage::compute_max_size();

    // With 4KB page, header, and sibling pointers, should fit ~200+ entries
    EXPECT_GT(max_size, 200);
    EXPECT_LT(max_size, 500);  // Sanity check
}

TEST_F(BPTreePageTest, ComputeInternalMaxSize) {
    uint32_t max_size = BPTreeInternalPage::compute_max_size();

    // Should be similar to leaf
    EXPECT_GT(max_size, 200);
    EXPECT_LT(max_size, 500);
}

// ─────────────────────────────────────────────────────────────────────────────
// Leaf Page Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(BPTreePageTest, LeafInsertSingle) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(100);

    BPTreeValue rid(10, 5);
    EXPECT_TRUE(leaf.insert(42, rid));

    EXPECT_EQ(leaf.num_keys(), 1);
    EXPECT_EQ(leaf.key_at(0), 42);

    BPTreeValue retrieved = leaf.value_at(0);
    EXPECT_EQ(retrieved.page_id, 10);
    EXPECT_EQ(retrieved.slot_id, 5);
}

TEST_F(BPTreePageTest, LeafInsertMultiple) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(100);

    // Insert out of order
    leaf.insert(30, BPTreeValue(1, 0));
    leaf.insert(10, BPTreeValue(2, 0));
    leaf.insert(50, BPTreeValue(3, 0));
    leaf.insert(20, BPTreeValue(4, 0));
    leaf.insert(40, BPTreeValue(5, 0));

    EXPECT_EQ(leaf.num_keys(), 5);

    // Should be sorted
    EXPECT_EQ(leaf.key_at(0), 10);
    EXPECT_EQ(leaf.key_at(1), 20);
    EXPECT_EQ(leaf.key_at(2), 30);
    EXPECT_EQ(leaf.key_at(3), 40);
    EXPECT_EQ(leaf.key_at(4), 50);

    // Values should match
    EXPECT_EQ(leaf.value_at(0).page_id, 2);
    EXPECT_EQ(leaf.value_at(1).page_id, 4);
    EXPECT_EQ(leaf.value_at(2).page_id, 1);
    EXPECT_EQ(leaf.value_at(3).page_id, 5);
    EXPECT_EQ(leaf.value_at(4).page_id, 3);
}

TEST_F(BPTreePageTest, LeafInsertDuplicate) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(100);

    EXPECT_TRUE(leaf.insert(42, BPTreeValue(1, 0)));
    EXPECT_FALSE(leaf.insert(42, BPTreeValue(2, 0)));  // Duplicate

    EXPECT_EQ(leaf.num_keys(), 1);
}

TEST_F(BPTreePageTest, LeafInsertFull) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(5);  // Small max size for testing

    for (int i = 0; i < 5; ++i) {
        EXPECT_TRUE(leaf.insert(i * 10, BPTreeValue(i, 0)));
    }

    EXPECT_TRUE(leaf.is_full());
    EXPECT_FALSE(leaf.insert(100, BPTreeValue(100, 0)));
}

TEST_F(BPTreePageTest, LeafFind) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(100);

    leaf.insert(10, BPTreeValue(1, 1));
    leaf.insert(20, BPTreeValue(2, 2));
    leaf.insert(30, BPTreeValue(3, 3));

    auto result1 = leaf.find(20);
    ASSERT_TRUE(result1.has_value());
    EXPECT_EQ(result1->page_id, 2);
    EXPECT_EQ(result1->slot_id, 2);

    auto result2 = leaf.find(25);
    EXPECT_FALSE(result2.has_value());

    auto result3 = leaf.find(10);
    ASSERT_TRUE(result3.has_value());
    EXPECT_EQ(result3->page_id, 1);
}

TEST_F(BPTreePageTest, LeafRemove) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(100);

    leaf.insert(10, BPTreeValue(1, 0));
    leaf.insert(20, BPTreeValue(2, 0));
    leaf.insert(30, BPTreeValue(3, 0));

    EXPECT_TRUE(leaf.remove(20));
    EXPECT_EQ(leaf.num_keys(), 2);

    // Remaining keys should be 10 and 30
    EXPECT_EQ(leaf.key_at(0), 10);
    EXPECT_EQ(leaf.key_at(1), 30);

    EXPECT_FALSE(leaf.remove(20));  // Already removed
}

TEST_F(BPTreePageTest, LeafRemoveFirst) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(100);

    leaf.insert(10, BPTreeValue(1, 0));
    leaf.insert(20, BPTreeValue(2, 0));
    leaf.insert(30, BPTreeValue(3, 0));

    EXPECT_TRUE(leaf.remove(10));
    EXPECT_EQ(leaf.num_keys(), 2);
    EXPECT_EQ(leaf.key_at(0), 20);
    EXPECT_EQ(leaf.key_at(1), 30);
}

TEST_F(BPTreePageTest, LeafRemoveLast) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(100);

    leaf.insert(10, BPTreeValue(1, 0));
    leaf.insert(20, BPTreeValue(2, 0));
    leaf.insert(30, BPTreeValue(3, 0));

    EXPECT_TRUE(leaf.remove(30));
    EXPECT_EQ(leaf.num_keys(), 2);
    EXPECT_EQ(leaf.key_at(0), 10);
    EXPECT_EQ(leaf.key_at(1), 20);
}

TEST_F(BPTreePageTest, LeafSiblingPointers) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(100);

    EXPECT_EQ(leaf.next_leaf_id(), INVALID_PAGE_ID);
    EXPECT_EQ(leaf.prev_leaf_id(), INVALID_PAGE_ID);

    leaf.set_next_leaf_id(5);
    leaf.set_prev_leaf_id(3);

    EXPECT_EQ(leaf.next_leaf_id(), 5);
    EXPECT_EQ(leaf.prev_leaf_id(), 3);
}

TEST_F(BPTreePageTest, LeafFindKeyIndex) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(100);

    leaf.insert(10, BPTreeValue(1, 0));
    leaf.insert(20, BPTreeValue(2, 0));
    leaf.insert(30, BPTreeValue(3, 0));

    bool found;
    EXPECT_EQ(leaf.find_key_index(10, &found), 0);
    EXPECT_TRUE(found);

    EXPECT_EQ(leaf.find_key_index(20, &found), 1);
    EXPECT_TRUE(found);

    EXPECT_EQ(leaf.find_key_index(15, &found), 1);  // Would go between 10 and 20
    EXPECT_FALSE(found);

    EXPECT_EQ(leaf.find_key_index(5, &found), 0);   // Would go before 10
    EXPECT_FALSE(found);

    EXPECT_EQ(leaf.find_key_index(35, &found), 3);  // Would go after 30
    EXPECT_FALSE(found);
}

TEST_F(BPTreePageTest, LeafMoveHalfTo) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(100);

    // Insert 10 entries
    for (int i = 0; i < 10; ++i) {
        leaf.insert(i * 10, BPTreeValue(i, 0));
    }
    EXPECT_EQ(leaf.num_keys(), 10);

    // Create sibling
    auto sibling_page = std::make_unique<Page>();
    sibling_page->set_page_id(2);
    BPTreeLeafPage sibling(sibling_page.get());
    sibling.init(100);

    // Move half
    BPTreeKey split_key = leaf.move_half_to(&sibling);

    EXPECT_EQ(leaf.num_keys(), 5);      // First half stays
    EXPECT_EQ(sibling.num_keys(), 5);   // Second half moved

    EXPECT_EQ(split_key, 50);  // First key of sibling

    // Verify original has 0-40
    EXPECT_EQ(leaf.key_at(0), 0);
    EXPECT_EQ(leaf.key_at(4), 40);

    // Verify sibling has 50-90
    EXPECT_EQ(sibling.key_at(0), 50);
    EXPECT_EQ(sibling.key_at(4), 90);
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal Page Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(BPTreePageTest, InternalSetChildren) {
    BPTreeInternalPage internal(page_.get());
    internal.init(100);

    internal.set_child_at(0, 10);
    internal.set_num_keys(0);

    EXPECT_EQ(internal.child_at(0), 10);
}

TEST_F(BPTreePageTest, InternalInsertAt) {
    BPTreeInternalPage internal(page_.get());
    internal.init(100);

    // Set up initial child
    internal.set_child_at(0, 100);

    // Insert first key with right child
    EXPECT_TRUE(internal.insert_at(0, 50, 101));
    EXPECT_EQ(internal.num_keys(), 1);
    EXPECT_EQ(internal.child_at(0), 100);  // Left child
    EXPECT_EQ(internal.key_at(0), 50);
    EXPECT_EQ(internal.child_at(1), 101);  // Right child

    // Insert another key
    EXPECT_TRUE(internal.insert_at(1, 75, 102));
    EXPECT_EQ(internal.num_keys(), 2);
    EXPECT_EQ(internal.key_at(1), 75);
    EXPECT_EQ(internal.child_at(2), 102);

    // Insert at beginning
    EXPECT_TRUE(internal.insert_at(0, 25, 103));
    EXPECT_EQ(internal.num_keys(), 3);
    EXPECT_EQ(internal.key_at(0), 25);
    EXPECT_EQ(internal.key_at(1), 50);
    EXPECT_EQ(internal.key_at(2), 75);
}

TEST_F(BPTreePageTest, InternalFindChildIndex) {
    BPTreeInternalPage internal(page_.get());
    internal.init(100);

    // Set up: children: [c0] key:25 [c1] key:50 [c2] key:75 [c3]
    internal.set_child_at(0, 100);
    internal.insert_at(0, 25, 101);
    internal.insert_at(1, 50, 102);
    internal.insert_at(2, 75, 103);

    // Keys < 25 should go to child 0
    EXPECT_EQ(internal.find_child_index(10), 0);
    EXPECT_EQ(internal.find_child_index(24), 0);

    // Keys 25-49 should go to child 1
    EXPECT_EQ(internal.find_child_index(25), 1);
    EXPECT_EQ(internal.find_child_index(30), 1);
    EXPECT_EQ(internal.find_child_index(49), 1);

    // Keys 50-74 should go to child 2
    EXPECT_EQ(internal.find_child_index(50), 2);
    EXPECT_EQ(internal.find_child_index(60), 2);

    // Keys >= 75 should go to child 3
    EXPECT_EQ(internal.find_child_index(75), 3);
    EXPECT_EQ(internal.find_child_index(100), 3);
}

TEST_F(BPTreePageTest, InternalRemoveAt) {
    BPTreeInternalPage internal(page_.get());
    internal.init(100);

    internal.set_child_at(0, 100);
    internal.insert_at(0, 25, 101);
    internal.insert_at(1, 50, 102);
    internal.insert_at(2, 75, 103);

    EXPECT_EQ(internal.num_keys(), 3);

    // Remove middle key
    internal.remove_at(1);
    EXPECT_EQ(internal.num_keys(), 2);
    EXPECT_EQ(internal.key_at(0), 25);
    EXPECT_EQ(internal.key_at(1), 75);
    EXPECT_EQ(internal.child_at(0), 100);
    EXPECT_EQ(internal.child_at(1), 101);
    EXPECT_EQ(internal.child_at(2), 103);
}

TEST_F(BPTreePageTest, InternalFullCheck) {
    BPTreeInternalPage internal(page_.get());
    internal.init(3);  // Small max for testing

    internal.set_child_at(0, 100);

    EXPECT_FALSE(internal.is_full());

    internal.insert_at(0, 25, 101);
    internal.insert_at(1, 50, 102);
    internal.insert_at(2, 75, 103);

    EXPECT_TRUE(internal.is_full());
    EXPECT_FALSE(internal.insert_at(3, 100, 104));  // Should fail
}

// ─────────────────────────────────────────────────────────────────────────────
// Underflow/Overflow Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(BPTreePageTest, MinSizeCalculation) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(10);

    EXPECT_EQ(leaf.min_size(), 5);
}

TEST_F(BPTreePageTest, UnderflowDetection) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(10);

    // Root node should never underflow
    EXPECT_FALSE(leaf.is_underflow());

    // Set parent to make it non-root
    leaf.set_parent_page_id(5);

    // With 0 keys, should be underflow (min is 5)
    EXPECT_TRUE(leaf.is_underflow());

    // Add entries
    for (int i = 0; i < 5; ++i) {
        leaf.insert(i, BPTreeValue(i, 0));
    }
    EXPECT_FALSE(leaf.is_underflow());  // At min

    // Remove one
    leaf.remove(0);
    EXPECT_TRUE(leaf.is_underflow());  // Below min
}

// ─────────────────────────────────────────────────────────────────────────────
// Page ID Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(BPTreePageTest, PageIdAccess) {
    BPTreeLeafPage leaf(page_.get());
    leaf.init(100);

    EXPECT_EQ(leaf.page_id(), 1);
}

}  // namespace
}  // namespace entropy
