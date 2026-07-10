/**
 * @file disk_manager_test.cpp
 * @brief Unit tests for DiskManager class
 */

#include <gtest/gtest.h>

#include <cstring>

#include "storage/disk_manager.hpp"
#include "test_utils.hpp"

namespace entropy {
namespace {

class DiskManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        temp_file_ = std::make_unique<test::TempFile>("dm_test_");
        disk_manager_ = std::make_unique<FileDiskManager>(temp_file_->string());
    }

    void TearDown() override {
        disk_manager_.reset();
        temp_file_.reset();
    }

    std::unique_ptr<test::TempFile> temp_file_;
    std::unique_ptr<DiskManager> disk_manager_;
};

TEST_F(DiskManagerTest, AllocatePage) {
    page_id_t page_id = disk_manager_->allocate_page();
    EXPECT_EQ(page_id, 0);

    page_id = disk_manager_->allocate_page();
    EXPECT_EQ(page_id, 1);

    page_id = disk_manager_->allocate_page();
    EXPECT_EQ(page_id, 2);
}

TEST_F(DiskManagerTest, WriteAndReadPage) {
    char write_data[config::kDefaultPageSize];
    char read_data[config::kDefaultPageSize];

    // Fill with test pattern
    for (size_t i = 0; i < config::kDefaultPageSize; ++i) {
        write_data[i] = static_cast<char>(i % 256);
    }

    page_id_t page_id = disk_manager_->allocate_page();

    auto status = disk_manager_->write_page(page_id, write_data);
    EXPECT_TRUE(status.ok()) << status.to_string();

    status = disk_manager_->read_page(page_id, read_data);
    EXPECT_TRUE(status.ok()) << status.to_string();

    EXPECT_EQ(std::memcmp(write_data, read_data, config::kDefaultPageSize), 0);
}

TEST_F(DiskManagerTest, MultiplePages) {
    const int num_pages = 10;
    std::vector<page_id_t> page_ids;

    // Allocate and write pages
    for (size_t i = 0; i < static_cast<size_t>(num_pages); ++i) {
        char write_data[config::kDefaultPageSize];
        std::memset(write_data, static_cast<int>(i), config::kDefaultPageSize);

        page_id_t page_id = disk_manager_->allocate_page();
        page_ids.push_back(page_id);

        auto status = disk_manager_->write_page(page_id, write_data);
        EXPECT_TRUE(status.ok());
    }

    // Verify each page
    for (size_t i = 0; i < static_cast<size_t>(num_pages); ++i) {
        char read_data[config::kDefaultPageSize];
        char expected[config::kDefaultPageSize];
        std::memset(expected, static_cast<int>(i), config::kDefaultPageSize);

        auto status = disk_manager_->read_page(page_ids[i], read_data);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(std::memcmp(read_data, expected, config::kDefaultPageSize), 0);
    }
}

TEST_F(DiskManagerTest, Persistence) {
    char write_data[config::kDefaultPageSize];
    std::memset(write_data, 'X', config::kDefaultPageSize);

    page_id_t page_id = disk_manager_->allocate_page();
    auto status = disk_manager_->write_page(page_id, write_data);
    EXPECT_TRUE(status.ok());

    // Close and reopen
    disk_manager_.reset();
    disk_manager_ = std::make_unique<FileDiskManager>(temp_file_->string());

    char read_data[config::kDefaultPageSize];
    status = disk_manager_->read_page(page_id, read_data);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(std::memcmp(read_data, write_data, config::kDefaultPageSize), 0);
}

// Regression for issue #2: a beyond-EOF (short) read must not latch the
// fstream into a permanent fail state that breaks all subsequent I/O.
TEST_F(DiskManagerTest, BeyondEofReadDoesNotLatchSubsequentIo) {
    char write_data[config::kDefaultPageSize];
    char read_data[config::kDefaultPageSize];
    std::memset(write_data, 'A', config::kDefaultPageSize);

    page_id_t page_id = disk_manager_->allocate_page();
    auto status = disk_manager_->write_page(page_id, write_data);
    ASSERT_TRUE(status.ok()) << status.to_string();

    // Beyond-EOF read is intentional (zero-fill); must succeed.
    char beyond_eof[config::kDefaultPageSize];
    std::memset(beyond_eof, 0xFF, config::kDefaultPageSize);
    status = disk_manager_->read_page(/*page_id=*/100, beyond_eof);
    ASSERT_TRUE(status.ok()) << status.to_string();
    for (size_t i = 0; i < config::kDefaultPageSize; ++i) {
        EXPECT_EQ(beyond_eof[i], '\0') << "byte " << i;
    }

    // Subsequent read of an existing page must still work.
    status = disk_manager_->read_page(page_id, read_data);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(std::memcmp(write_data, read_data, config::kDefaultPageSize), 0);

    // Subsequent write must also work (same fstream failbit latch).
    char write_data2[config::kDefaultPageSize];
    std::memset(write_data2, 'B', config::kDefaultPageSize);
    page_id_t page_id2 = disk_manager_->allocate_page();
    status = disk_manager_->write_page(page_id2, write_data2);
    ASSERT_TRUE(status.ok()) << status.to_string();

    status = disk_manager_->read_page(page_id2, read_data);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(std::memcmp(write_data2, read_data, config::kDefaultPageSize), 0);
}

// error_if_exists=true must refuse to open a file that already exists,
// leaving the manager closed (is_open() == false).
TEST(FileDiskManagerOptionsTest, ErrorIfExistsFailsOnExistingFile) {
    test::TempFile temp("dm_exists_");

    // Create the file with a first manager.
    {
        FileDiskManager creator(temp.string());
        ASSERT_TRUE(creator.is_open());
    }

    FileDiskManager dm(temp.string(), /*create_if_missing=*/true,
                       /*error_if_exists=*/true);
    EXPECT_FALSE(dm.is_open());
}

// create_if_missing=false must refuse to create a file that does not exist,
// leaving the manager closed (is_open() == false).
TEST(FileDiskManagerOptionsTest, CreateIfMissingFalseFailsOnMissingFile) {
    test::TempFile temp("dm_missing_");  // path only; file is not created

    FileDiskManager dm(temp.string(), /*create_if_missing=*/false);
    EXPECT_FALSE(dm.is_open());
}

// A deallocated page id is handed back by the next allocate_page before the
// file is grown.
TEST_F(DiskManagerTest, DeallocateThenAllocateReusesPageId) {
    page_id_t p0 = disk_manager_->allocate_page();
    page_id_t p1 = disk_manager_->allocate_page();
    page_id_t p2 = disk_manager_->allocate_page();

    disk_manager_->deallocate_page(p1);

    // The freed id is reused before the file grows.
    page_id_t reused = disk_manager_->allocate_page();
    EXPECT_EQ(reused, p1);

    // Once the free list is empty, allocation grows the file again.
    page_id_t grown = disk_manager_->allocate_page();
    EXPECT_EQ(grown, p2 + 1);
}

}  // namespace
}  // namespace entropy
