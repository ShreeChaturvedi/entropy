#pragma once

/**
 * @file buffer_pool.hpp
 * @brief Buffer Pool Manager
 */

#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "common/types.hpp"
#include "entropy/status.hpp"
#include "storage/disk_manager.hpp"
#include "storage/lru_replacer.hpp"
#include "storage/page.hpp"

namespace entropy {

/**
 * @brief Buffer Pool Manager
 *
 * Manages a pool of in-memory pages, caching disk pages and handling
 * eviction when the pool is full.
 */
class BufferPoolManager {
public:
    /**
     * @brief Construct a Buffer Pool Manager
     * @param pool_size Number of pages in the pool
     * @param disk_manager Disk manager for I/O operations
     */
    BufferPoolManager(size_t pool_size, std::shared_ptr<DiskManager> disk_manager);

    ~BufferPoolManager();

    // Non-copyable, non-movable
    BufferPoolManager(const BufferPoolManager&) = delete;
    BufferPoolManager& operator=(const BufferPoolManager&) = delete;
    BufferPoolManager(BufferPoolManager&&) = delete;
    BufferPoolManager& operator=(BufferPoolManager&&) = delete;

    /**
     * @brief Fetch a page from the buffer pool
     * @param page_id The page to fetch
     * @return Pointer to the page, or nullptr if failed
     */
    [[nodiscard]] Page* fetch_page(page_id_t page_id);

    /**
     * @brief Unpin a page in the buffer pool
     * @param page_id The page to unpin
     * @param is_dirty Whether the page was modified
     * @return true if successful
     */
    bool unpin_page(page_id_t page_id, bool is_dirty);

    /**
     * @brief Flush a page to disk
     * @param page_id The page to flush
     * @return true if successful
     */
    bool flush_page(page_id_t page_id);

    /**
     * @brief Create a new page in the buffer pool
     * @param page_id Output parameter for the new page ID
     * @return Pointer to the new page, or nullptr if failed
     */
    [[nodiscard]] Page* new_page(page_id_t* page_id);

    /**
     * @brief Delete a page from the buffer pool
     * @param page_id The page to delete
     * @return true if successful
     */
    bool delete_page(page_id_t page_id);

    /**
     * @brief Flush all pages to disk
     */
    void flush_all_pages();

    /**
     * @brief Get the pool size
     */
    [[nodiscard]] size_t pool_size() const noexcept { return pool_size_; }

    /**
     * @brief Number of free frames currently available
     *
     * Increases when delete_page reclaims a buffered page. Useful for
     * detecting page leaks after B+ tree merges.
     */
    [[nodiscard]] size_t free_list_size() const;

    /**
     * @brief Install a hook run immediately before any dirty page is written
     *
     * The hook is invoked with the page's LSN just before every disk
     * write_page (eviction, flush_page, flush_all_pages). It is the seam used
     * to enforce the WAL-before-page (steal) rule: flush the log up to the
     * page's LSN before the page reaches disk. If the hook returns a non-ok
     * Status the page write is skipped entirely and the error propagated —
     * a failed log flush must never be followed by writing the dirty page.
     * This is a mechanism only; the hook is empty until an owner installs one.
     *
     * The hook runs while the buffer pool mutex is held, including during
     * destruction (the destructor flushes all pages). It must not re-enter
     * the BufferPoolManager and must outlive the pool.
     */
    void set_wal_flush_hook(std::function<Status(lsn_t)> hook);

private:
    /// Find a frame to use (evict if necessary)
    [[nodiscard]] frame_id_t find_victim_frame();

    /// Write a page to disk, running the WAL flush hook first.
    [[nodiscard]] Status write_page_to_disk(Page* page);

    size_t pool_size_;
    std::shared_ptr<DiskManager> disk_manager_;
    std::vector<Page> pages_;
    std::unique_ptr<LRUReplacer> replacer_;
    std::unordered_map<page_id_t, frame_id_t> page_table_;
    std::vector<frame_id_t> free_list_;
    std::function<Status(lsn_t)> wal_flush_hook_;
    mutable std::mutex mutex_;
};

}  // namespace entropy
