/**
 * @file disk_manager_test.cpp
 * @brief Unit tests for DiskManager class
 */

#include <gtest/gtest.h>

#include <cstring>
#include <fstream>

#include "storage/disk_manager.hpp"
#include "storage/page.hpp"
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

// With checksums enabled, a clean page round-trips and verifies, while a
// torn/partial write (a byte flipped on disk after the stamp) is DETECTED at
// read time as a Corruption Status instead of being silently returned.
TEST(FileDiskManagerChecksumTest, RoundTripsAndDetectsTornPage) {
    test::TempFile temp("dm_checksum_");
    page_id_t page_id = 0;
    {
        FileDiskManager dm(temp.string(), /*create_if_missing=*/true,
                           /*error_if_exists=*/false, /*enable_checksums=*/true);
        ASSERT_TRUE(dm.is_open());
        page_id = dm.allocate_page();

        char page[config::kDefaultPageSize];
        std::memset(page, 'A', sizeof(page));
        ASSERT_TRUE(dm.write_page(page_id, page).ok());

        // A clean read verifies its stamp and succeeds.
        char read_back[config::kDefaultPageSize];
        EXPECT_TRUE(dm.read_page(page_id, read_back).ok());
    }

    // Simulate a torn write: flip one payload byte on disk, leaving the stamped
    // checksum stale. Offset 64 is well past the header checksum field (12-15).
    {
        std::fstream f(temp.string(),
                       std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(f.is_open());
        f.seekp(64, std::ios::beg);
        const char corrupt = 'B';  // the byte was 'A'
        f.write(&corrupt, 1);
        ASSERT_TRUE(f.good());
    }

    // Reopen and read: the stale checksum no longer matches the bytes.
    FileDiskManager dm(temp.string(), /*create_if_missing=*/true,
                       /*error_if_exists=*/false, /*enable_checksums=*/true);
    ASSERT_TRUE(dm.is_open());
    char read_back[config::kDefaultPageSize];
    Status s = dm.read_page(page_id, read_back);
    EXPECT_EQ(s.code(), StatusCode::kCorruption) << s.to_string();
}

// The checksum stamp lives in the page header, so a manager with checksums
// DISABLED (the default) round-trips arbitrary bytes untouched and never
// rejects them — the opt-in must not change behavior for existing callers.
TEST(FileDiskManagerChecksumTest, DisabledLeavesBytesUntouched) {
    test::TempFile temp("dm_nocksum_");
    FileDiskManager dm(temp.string());  // checksums off by default
    ASSERT_TRUE(dm.is_open());
    page_id_t page_id = dm.allocate_page();

    char page[config::kDefaultPageSize];
    for (size_t i = 0; i < sizeof(page); ++i) {
        page[i] = static_cast<char>((i * 7 + 1) % 256);
    }
    ASSERT_TRUE(dm.write_page(page_id, page).ok());

    char read_back[config::kDefaultPageSize];
    ASSERT_TRUE(dm.read_page(page_id, read_back).ok());
    EXPECT_EQ(std::memcmp(page, read_back, sizeof(page)), 0)
        << "checksums-off must preserve every byte, header included";
}

// A deallocated page id is handed back by the next allocate_page before the
// file is grown.
TEST_F(DiskManagerTest, DeallocateThenAllocateReusesPageId) {
    page_id_t p0 = disk_manager_->allocate_page();
    page_id_t p1 = disk_manager_->allocate_page();
    page_id_t p2 = disk_manager_->allocate_page();
    EXPECT_NE(p0, p1);

    disk_manager_->deallocate_page(p1);

    // The freed id is reused before the file grows.
    page_id_t reused = disk_manager_->allocate_page();
    EXPECT_EQ(reused, p1);

    // Once the free list is empty, allocation grows the file again.
    page_id_t grown = disk_manager_->allocate_page();
    EXPECT_EQ(grown, p2 + 1);
}

// A double deallocation must be rejected: the id may only enter the free
// list once, so subsequent allocations return distinct ids.
TEST_F(DiskManagerTest, DoubleDeallocateIsRejected) {
    page_id_t p0 = disk_manager_->allocate_page();
    page_id_t p1 = disk_manager_->allocate_page();

    disk_manager_->deallocate_page(p0);
    disk_manager_->deallocate_page(p0);  // Ignored: already on the free list.

    page_id_t a = disk_manager_->allocate_page();
    page_id_t b = disk_manager_->allocate_page();
    EXPECT_NE(a, b);
    EXPECT_EQ(a, p0);
    EXPECT_EQ(b, p1 + 1);

    // Out-of-range ids are rejected too and never enter the free list.
    disk_manager_->deallocate_page(-1);
    disk_manager_->deallocate_page(1000);
    EXPECT_EQ(disk_manager_->allocate_page(), b + 1);
}

// A reused page id must read back zeroed, like a fresh page, not with the
// stale bytes of its previous life.
TEST_F(DiskManagerTest, ReusedPageIdReadsBackZeroed) {
    char write_data[config::kDefaultPageSize];
    std::memset(write_data, 'S', config::kDefaultPageSize);

    page_id_t page_id = disk_manager_->allocate_page();
    auto status = disk_manager_->write_page(page_id, write_data);
    ASSERT_TRUE(status.ok()) << status.to_string();

    disk_manager_->deallocate_page(page_id);

    page_id_t reused = disk_manager_->allocate_page();
    ASSERT_EQ(reused, page_id);

    char read_data[config::kDefaultPageSize];
    status = disk_manager_->read_page(reused, read_data);
    ASSERT_TRUE(status.ok()) << status.to_string();
    for (size_t i = 0; i < config::kDefaultPageSize; ++i) {
        ASSERT_EQ(read_data[i], '\0') << "byte " << i;
    }
}

}  // namespace
}  // namespace entropy
