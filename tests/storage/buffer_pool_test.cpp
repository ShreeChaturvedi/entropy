/**
 * @file buffer_pool_test.cpp
 * @brief Unit tests for BufferPoolManager
 */

#include <gtest/gtest.h>

#include "storage/buffer_pool.hpp"
#include "test_utils.hpp"

namespace entropy {
namespace {

class BufferPoolTest : public ::testing::Test {
protected:
    void SetUp() override {
        temp_file_ = std::make_unique<test::TempFile>("bp_test_");
        disk_manager_ = std::make_shared<DiskManager>(temp_file_->string());
        buffer_pool_ = std::make_unique<BufferPoolManager>(pool_size_, disk_manager_);
    }

    void TearDown() override {
        buffer_pool_.reset();
        disk_manager_.reset();
        temp_file_.reset();
    }

    static constexpr size_t pool_size_ = 10;
    std::unique_ptr<test::TempFile> temp_file_;
    std::shared_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_;
};

TEST_F(BufferPoolTest, NewPage) {
    page_id_t page_id;
    Page* page = buffer_pool_->new_page(&page_id);

    ASSERT_NE(page, nullptr);
    EXPECT_EQ(page_id, 0);
    EXPECT_EQ(page->page_id(), page_id);
}

TEST_F(BufferPoolTest, FetchPage) {
    page_id_t page_id;
    Page* page = buffer_pool_->new_page(&page_id);
    ASSERT_NE(page, nullptr);

    // Write some data
    char* data = page->data();
    data[config::kPageHeaderSize] = 'T';

    buffer_pool_->unpin_page(page_id, true);

    // Fetch it back
    Page* fetched = buffer_pool_->fetch_page(page_id);
    ASSERT_NE(fetched, nullptr);
    EXPECT_EQ(fetched->data()[config::kPageHeaderSize], 'T');

    buffer_pool_->unpin_page(page_id, false);
}

TEST_F(BufferPoolTest, UnpinPage) {
    page_id_t page_id;
    Page* page = buffer_pool_->new_page(&page_id);
    ASSERT_NE(page, nullptr);

    EXPECT_TRUE(buffer_pool_->unpin_page(page_id, false));
    EXPECT_FALSE(buffer_pool_->unpin_page(page_id, false));  // Already unpinned
}

TEST_F(BufferPoolTest, FlushPage) {
    page_id_t page_id;
    Page* page = buffer_pool_->new_page(&page_id);
    ASSERT_NE(page, nullptr);

    page->data()[config::kPageHeaderSize] = 'F';
    page->set_dirty(true);

    buffer_pool_->unpin_page(page_id, true);
    EXPECT_TRUE(buffer_pool_->flush_page(page_id));
}

TEST_F(BufferPoolTest, PoolEviction) {
    std::vector<page_id_t> page_ids;

    // Create more pages than pool can hold
    for (size_t i = 0; i < pool_size_ + 5; ++i) {
        page_id_t page_id;
        Page* page = buffer_pool_->new_page(&page_id);

        if (page != nullptr) {
            page_ids.push_back(page_id);
            buffer_pool_->unpin_page(page_id, true);
        }
    }

    // All pages should have been created (with eviction)
    EXPECT_EQ(page_ids.size(), pool_size_ + 5);
}

}  // namespace
}  // namespace entropy
