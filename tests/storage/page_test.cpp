/**
 * @file page_test.cpp
 * @brief Unit tests for Page class
 */

#include <gtest/gtest.h>

#include "storage/page.hpp"

namespace entropy {
namespace {

TEST(PageTest, DefaultConstruction) {
    Page page;

    EXPECT_EQ(page.page_id(), INVALID_PAGE_ID);
    EXPECT_EQ(page.page_type(), PageType::INVALID);
    EXPECT_FALSE(page.is_dirty());
    EXPECT_EQ(page.pin_count(), 0);
}

TEST(PageTest, SetPageId) {
    Page page;
    page.set_page_id(42);

    EXPECT_EQ(page.page_id(), 42);
}

TEST(PageTest, SetPageType) {
    Page page;
    page.set_page_type(PageType::TABLE_PAGE);

    EXPECT_EQ(page.page_type(), PageType::TABLE_PAGE);
}

TEST(PageTest, DirtyFlag) {
    Page page;

    EXPECT_FALSE(page.is_dirty());

    page.set_dirty(true);
    EXPECT_TRUE(page.is_dirty());

    page.set_dirty(false);
    EXPECT_FALSE(page.is_dirty());
}

TEST(PageTest, PinCount) {
    Page page;

    EXPECT_EQ(page.pin_count(), 0);

    page.pin();
    EXPECT_EQ(page.pin_count(), 1);

    page.pin();
    EXPECT_EQ(page.pin_count(), 2);

    page.unpin();
    EXPECT_EQ(page.pin_count(), 1);

    page.unpin();
    EXPECT_EQ(page.pin_count(), 0);

    // Unpin should not go negative
    page.unpin();
    EXPECT_EQ(page.pin_count(), 0);
}

TEST(PageTest, Reset) {
    Page page;
    page.set_page_id(42);
    page.set_page_type(PageType::TABLE_PAGE);
    page.set_dirty(true);
    page.pin();

    page.reset();

    EXPECT_EQ(page.page_id(), INVALID_PAGE_ID);
    EXPECT_EQ(page.page_type(), PageType::INVALID);
    EXPECT_FALSE(page.is_dirty());
    EXPECT_EQ(page.pin_count(), 0);
}

TEST(PageTest, LSN) {
    Page page;

    EXPECT_EQ(page.lsn(), INVALID_LSN);

    page.set_lsn(100);
    EXPECT_EQ(page.lsn(), 100);
}

TEST(PageTest, DataAccess) {
    Page page;

    // Write some data
    char* data = page.data();
    data[config::kPageHeaderSize] = 'A';
    data[config::kPageHeaderSize + 1] = 'B';
    data[config::kPageHeaderSize + 2] = 'C';

    // Verify read
    const char* const_data = page.data();
    EXPECT_EQ(const_data[config::kPageHeaderSize], 'A');
    EXPECT_EQ(const_data[config::kPageHeaderSize + 1], 'B');
    EXPECT_EQ(const_data[config::kPageHeaderSize + 2], 'C');
}

}  // namespace
}  // namespace entropy
