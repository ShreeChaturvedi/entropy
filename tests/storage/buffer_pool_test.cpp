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

// In-memory disk manager that records every deallocate_page call so a test can
// assert that delete_page frees the on-disk id even when the page is no longer
// cached in the buffer pool.
class DeallocRecordingDiskManager : public DiskManager {
public:
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
        std::memcpy(pages_[page_id].data(), page_data, page_size());
        if (page_id >= num_pages_) {
            num_pages_ = page_id + 1;
        }
        return Status::Ok();
    }

    page_id_t allocate_page() override { return num_pages_++; }
    void deallocate_page(page_id_t page_id) override {
        deallocated.push_back(page_id);
    }
    page_id_t num_pages() const noexcept override { return num_pages_; }
    bool is_open() const noexcept override { return true; }
    void sync() override {}

    std::vector<page_id_t> deallocated;

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

// Regression: when an eviction flush fails, the victim frame must be returned
// to the replacer, not the free list. The old code pushed it to the free list
// while its page id still mapped to that frame; a later new_page could then pop
// the same frame and reset() it even though a re-fetch had pinned the page,
// corrupting live data under the pin. A transient WAL-flush-hook failure makes
// the eviction flush fail in normal operation.
TEST(BufferPoolEvictionFailureTest, FailedEvictionKeepsVictimFrameOutOfFreeList) {
    auto dm = std::make_shared<RecordingDiskManager>();
    BufferPoolManager bpm(/*pool_size=*/1, dm);

    // Page 0: sentinel byte, dirty, unpinned -> the single evictable frame.
    page_id_t p0;
    Page* page0 = bpm.new_page(&p0);
    ASSERT_NE(page0, nullptr);
    page0->set_lsn(5);
    page0->data()[config::kPageHeaderSize] = 'A';
    bpm.unpin_page(p0, /*is_dirty=*/true);

    // Fail the next page write via the WAL flush hook so evicting page 0 fails.
    bpm.set_wal_flush_hook(
        [](lsn_t) { return Status::IOError("wal flush failed"); });

    // Admitting a new page must evict + flush page 0, which now fails.
    page_id_t p1;
    Page* page1 = bpm.new_page(&p1);
    EXPECT_EQ(page1, nullptr);

    // Allow writes again.
    bpm.set_wal_flush_hook(nullptr);

    // Page 0 is still mapped: re-fetch pins it and its data is intact.
    Page* refetched = bpm.fetch_page(p0);
    ASSERT_NE(refetched, nullptr);
    EXPECT_EQ(refetched->data()[config::kPageHeaderSize], 'A');

    // With the bug, frame 0 also sits on the free list, so this new_page pops it
    // and reset()s page 0 under our pin. With the fix, frame 0 is pinned and not
    // free, so no victim is available and new_page fails, leaving page 0 intact.
    page_id_t p2;
    Page* page2 = bpm.new_page(&p2);
    EXPECT_EQ(page2, nullptr) << "new_page reused the still-pinned victim frame";
    EXPECT_EQ(refetched->data()[config::kPageHeaderSize], 'A')
        << "victim page was reset while pinned";

    bpm.unpin_page(p0, /*is_dirty=*/false);
}

// Direct accounting check for the eviction-failure double-listing bug: after a
// failed eviction flush, the victim frame must remain out of the free list. The
// old code pushed the still-mapped frame onto the free list, so with a single-
// frame pool free_list_size() would read 1 while the frame is still in the page
// table — the frame double-listed. The fix returns it to the replacer instead,
// leaving the free list empty.
TEST(BufferPoolEvictionFailureTest, FailedEvictionLeavesFreeListEmpty) {
    auto dm = std::make_shared<RecordingDiskManager>();
    BufferPoolManager bpm(/*pool_size=*/1, dm);

    page_id_t p0;
    Page* page0 = bpm.new_page(&p0);
    ASSERT_NE(page0, nullptr);
    bpm.unpin_page(p0, /*is_dirty=*/true);  // dirty -> eviction must flush it
    EXPECT_EQ(bpm.free_list_size(), 0u);    // the single frame is in use

    // Make the eviction flush fail.
    bpm.set_wal_flush_hook(
        [](lsn_t) { return Status::IOError("wal flush failed"); });

    page_id_t p1;
    Page* page1 = bpm.new_page(&p1);
    EXPECT_EQ(page1, nullptr);

    // The victim stayed mapped and went back to the replacer, NOT the free list.
    EXPECT_EQ(bpm.free_list_size(), 0u)
        << "failed-eviction victim was double-listed onto the free list";
}

// Regression: delete_page must deallocate the on-disk page id even when the page
// is not currently cached in the buffer pool. The old code returned success
// without freeing the id for an uncached page, leaking disk space for pages that
// were evicted and then deleted (a normal event during B+ tree merges under
// memory pressure).
TEST(BufferPoolDeletePageTest, DeletesUncachedPageFreesDiskId) {
    auto dm = std::make_shared<DeallocRecordingDiskManager>();
    BufferPoolManager bpm(/*pool_size=*/1, dm);

    // Create page 0 (clean) and evict it by admitting page 1 into the one frame.
    page_id_t p0;
    Page* page0 = bpm.new_page(&p0);
    ASSERT_NE(page0, nullptr);
    bpm.unpin_page(p0, /*is_dirty=*/false);

    page_id_t p1;
    Page* page1 = bpm.new_page(&p1);
    ASSERT_NE(page1, nullptr);
    bpm.unpin_page(p1, /*is_dirty=*/false);

    // Page 0 is no longer cached, but deleting it must still free its disk id.
    EXPECT_TRUE(bpm.delete_page(p0, /*deallocate=*/true));
    ASSERT_EQ(dm->deallocated.size(), 1u)
        << "delete_page skipped disk deallocation for an uncached page";
    EXPECT_EQ(dm->deallocated[0], p0);
}

// The deferred-deallocation contract still holds for uncached pages: with
// deallocate=false the id is left allocated for the caller to free later.
TEST(BufferPoolDeletePageTest, DeletesUncachedPageDeferredKeepsDiskId) {
    auto dm = std::make_shared<DeallocRecordingDiskManager>();
    BufferPoolManager bpm(/*pool_size=*/1, dm);

    page_id_t p0;
    Page* page0 = bpm.new_page(&p0);
    ASSERT_NE(page0, nullptr);
    bpm.unpin_page(p0, /*is_dirty=*/false);

    page_id_t p1;
    Page* page1 = bpm.new_page(&p1);
    ASSERT_NE(page1, nullptr);
    bpm.unpin_page(p1, /*is_dirty=*/false);

    EXPECT_TRUE(bpm.delete_page(p0, /*deallocate=*/false));
    EXPECT_TRUE(dm->deallocated.empty())
        << "deferred delete_page must not free the disk id";
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
