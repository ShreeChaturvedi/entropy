/**
 * @file lru_replacer_test.cpp
 * @brief Unit tests for LRUReplacer
 */

#include <gtest/gtest.h>

#include "storage/lru_replacer.hpp"

namespace entropy {
namespace {

TEST(LRUReplacerTest, InitiallyEmpty) {
    LRUReplacer replacer(10);
    EXPECT_EQ(replacer.size(), 0);
}

TEST(LRUReplacerTest, Unpin) {
    LRUReplacer replacer(10);

    replacer.unpin(1);
    replacer.unpin(2);
    replacer.unpin(3);

    EXPECT_EQ(replacer.size(), 3);
}

TEST(LRUReplacerTest, Pin) {
    LRUReplacer replacer(10);

    replacer.unpin(1);
    replacer.unpin(2);
    replacer.unpin(3);

    replacer.pin(2);
    EXPECT_EQ(replacer.size(), 2);

    replacer.pin(1);
    EXPECT_EQ(replacer.size(), 1);
}

TEST(LRUReplacerTest, Evict) {
    LRUReplacer replacer(10);

    replacer.unpin(1);
    replacer.unpin(2);
    replacer.unpin(3);

    frame_id_t frame_id;

    // Should evict in LRU order (1 was added first)
    EXPECT_TRUE(replacer.evict(&frame_id));
    EXPECT_EQ(frame_id, 1);

    EXPECT_TRUE(replacer.evict(&frame_id));
    EXPECT_EQ(frame_id, 2);

    EXPECT_TRUE(replacer.evict(&frame_id));
    EXPECT_EQ(frame_id, 3);

    // No more to evict
    EXPECT_FALSE(replacer.evict(&frame_id));
}

TEST(LRUReplacerTest, EvictEmpty) {
    LRUReplacer replacer(10);

    frame_id_t frame_id;
    EXPECT_FALSE(replacer.evict(&frame_id));
}

TEST(LRUReplacerTest, DuplicateUnpin) {
    LRUReplacer replacer(10);

    replacer.unpin(1);
    replacer.unpin(1);  // Should not add duplicate

    EXPECT_EQ(replacer.size(), 1);
}

}  // namespace
}  // namespace entropy
