/**
 * @file buffer_pool_test.cpp
 * @brief Unit tests for BufferPoolManager
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/buffer_pool.hpp"
#include "test_utils.hpp"

namespace entropy {
namespace {

// In-memory disk manager that records the order of hook and write events so a
// test can assert the WAL flush hook runs before the page reaches disk.
class RecordingDiskManager : public DiskManager {
public:
    struct Event {
        std::string kind;  // "hook" or "write"
        lsn_t lsn;
    };

    Status read_page(page_id_t page_id, char* page_data) override {
        auto it = pages_.find(page_id);
        if (it == pages_.end()) {
            std::memset(page_data, 0, page_size());
        } else {
            std::memcpy(page_data, it->second.data(), page_size());
        }
        return Status::Ok();
    }

    Status write_page(page_id_t page_id, const char* page_data) override {
        lsn_t lsn = 0;
        std::memcpy(&lsn, page_data, sizeof(lsn));  // LSN lives at offset 0
        events.push_back({"write", lsn});
        std::memcpy(pages_[page_id].data(), page_data, page_size());
        if (page_id >= num_pages_) {
            num_pages_ = page_id + 1;
        }
        return Status::Ok();
    }

    page_id_t allocate_page() override { return num_pages_++; }
    void deallocate_page(page_id_t /*page_id*/) override {}
    page_id_t num_pages() const noexcept override { return num_pages_; }
    bool is_open() const noexcept override { return true; }
    void sync() override {}

    void note_hook(lsn_t lsn) { events.push_back({"hook", lsn}); }

    std::vector<Event> events;

private:
    std::unordered_map<page_id_t, std::array<char, DiskManager::page_size()>>
        pages_;
    page_id_t num_pages_ = 0;
};

class BufferPoolTest : public ::testing::Test {
protected:
    void SetUp() override {
        temp_file_ = std::make_unique<test::TempFile>("bp_test_");
        disk_manager_ = std::make_shared<FileDiskManager>(temp_file_->string());
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

// The WAL flush hook must run with the page's LSN immediately before the page
// is written to disk on an explicit flush_page.
TEST(BufferPoolWalHookTest, HookRunsWithPageLsnBeforeFlushWrite) {
    auto dm = std::make_shared<RecordingDiskManager>();
    BufferPoolManager bpm(4, dm);
    bpm.set_wal_flush_hook([rec = dm.get()](lsn_t lsn) {
        rec->note_hook(lsn);
        return Status::Ok();
    });

    page_id_t page_id;
    Page* page = bpm.new_page(&page_id);
    ASSERT_NE(page, nullptr);
    page->set_lsn(42);
    page->data()[config::kPageHeaderSize] = 'Z';
    bpm.unpin_page(page_id, /*is_dirty=*/true);

    ASSERT_TRUE(bpm.flush_page(page_id));

    ASSERT_EQ(dm->events.size(), 2u);
    EXPECT_EQ(dm->events[0].kind, "hook");
    EXPECT_EQ(dm->events[0].lsn, 42u);
    EXPECT_EQ(dm->events[1].kind, "write");
    EXPECT_EQ(dm->events[1].lsn, 42u);
}

// The hook must also fire before the write when a dirty page is evicted.
TEST(BufferPoolWalHookTest, HookRunsBeforeWriteOnEviction) {
    auto dm = std::make_shared<RecordingDiskManager>();
    BufferPoolManager bpm(/*pool_size=*/1, dm);
    bpm.set_wal_flush_hook([rec = dm.get()](lsn_t lsn) {
        rec->note_hook(lsn);
        return Status::Ok();
    });

    page_id_t p0;
    Page* page0 = bpm.new_page(&p0);
    ASSERT_NE(page0, nullptr);
    page0->set_lsn(7);
    bpm.unpin_page(p0, /*is_dirty=*/true);

    // A single-frame pool must evict (and flush) page 0 to admit page 1.
    page_id_t p1;
    Page* page1 = bpm.new_page(&p1);
    ASSERT_NE(page1, nullptr);

    ASSERT_EQ(dm->events.size(), 2u);
    EXPECT_EQ(dm->events[0].kind, "hook");
    EXPECT_EQ(dm->events[0].lsn, 7u);
    EXPECT_EQ(dm->events[1].kind, "write");
    EXPECT_EQ(dm->events[1].lsn, 7u);
}

// A failing hook (log flush error) must veto the page write entirely:
// no disk write may follow a failed WAL flush.
TEST(BufferPoolWalHookTest, HookErrorVetoesPageWrite) {
    auto dm = std::make_shared<RecordingDiskManager>();
    BufferPoolManager bpm(4, dm);
    bpm.set_wal_flush_hook([rec = dm.get()](lsn_t lsn) {
        rec->note_hook(lsn);
        return Status::IOError("log flush failed");
    });

    page_id_t page_id;
    Page* page = bpm.new_page(&page_id);
    ASSERT_NE(page, nullptr);
    page->set_lsn(99);
    bpm.unpin_page(page_id, /*is_dirty=*/true);

    EXPECT_FALSE(bpm.flush_page(page_id));

    // The hook ran, but the page write was skipped.
    ASSERT_EQ(dm->events.size(), 1u);
    EXPECT_EQ(dm->events[0].kind, "hook");
    EXPECT_EQ(dm->events[0].lsn, 99u);

    // Once the log flush succeeds, the (still dirty) page can be written.
    bpm.set_wal_flush_hook([rec = dm.get()](lsn_t lsn) {
        rec->note_hook(lsn);
        return Status::Ok();
    });
    EXPECT_TRUE(bpm.flush_page(page_id));
    ASSERT_EQ(dm->events.size(), 3u);
    EXPECT_EQ(dm->events[1].kind, "hook");
    EXPECT_EQ(dm->events[2].kind, "write");
    EXPECT_EQ(dm->events[2].lsn, 99u);
}

// The hook must also fire (before each write) on the flush_all_pages path.
TEST(BufferPoolWalHookTest, HookRunsBeforeWritesOnFlushAllPages) {
    auto dm = std::make_shared<RecordingDiskManager>();
    BufferPoolManager bpm(4, dm);
    bpm.set_wal_flush_hook([rec = dm.get()](lsn_t lsn) {
        rec->note_hook(lsn);
        return Status::Ok();
    });

    const std::array<lsn_t, 2> lsns = {11, 22};
    for (size_t i = 0; i < lsns.size(); ++i) {
        page_id_t page_id;
        Page* page = bpm.new_page(&page_id);
        ASSERT_NE(page, nullptr);
        page->set_lsn(lsns[i]);
        bpm.unpin_page(page_id, /*is_dirty=*/true);
    }

    bpm.flush_all_pages();

    // Each dirty page produces a hook immediately followed by its write,
    // with matching LSNs (page-table iteration order is unspecified).
    ASSERT_EQ(dm->events.size(), 4u);
    std::vector<lsn_t> flushed;
    for (size_t i = 0; i < dm->events.size(); i += 2) {
        EXPECT_EQ(dm->events[i].kind, "hook");
        EXPECT_EQ(dm->events[i + 1].kind, "write");
        EXPECT_EQ(dm->events[i].lsn, dm->events[i + 1].lsn);
        flushed.push_back(dm->events[i].lsn);
    }
    std::sort(flushed.begin(), flushed.end());
    EXPECT_EQ(flushed, std::vector<lsn_t>({11, 22}));
}

}  // namespace
}  // namespace entropy
