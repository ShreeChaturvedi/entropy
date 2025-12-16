/**
 * @file disk_manager_test.cpp
 * @brief Unit tests for DiskManager class
 */

#include <gtest/gtest.h>

#include "storage/disk_manager.hpp"
#include "test_utils.hpp"

namespace entropy {
namespace {

class DiskManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        temp_file_ = std::make_unique<test::TempFile>("dm_test_");
        disk_manager_ = std::make_unique<DiskManager>(temp_file_->string());
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
    for (int i = 0; i < num_pages; ++i) {
        char write_data[config::kDefaultPageSize];
        std::memset(write_data, i, config::kDefaultPageSize);

        page_id_t page_id = disk_manager_->allocate_page();
        page_ids.push_back(page_id);

        auto status = disk_manager_->write_page(page_id, write_data);
        EXPECT_TRUE(status.ok());
    }

    // Verify each page
    for (int i = 0; i < num_pages; ++i) {
        char read_data[config::kDefaultPageSize];
        char expected[config::kDefaultPageSize];
        std::memset(expected, i, config::kDefaultPageSize);

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
    disk_manager_ = std::make_unique<DiskManager>(temp_file_->string());

    char read_data[config::kDefaultPageSize];
    status = disk_manager_->read_page(page_id, read_data);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(std::memcmp(read_data, write_data, config::kDefaultPageSize), 0);
}

}  // namespace
}  // namespace entropy
