#pragma once

/**
 * @file buffer_pool.hpp
 * @brief Buffer Pool Manager
 */

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

private:
    /// Find a frame to use (evict if necessary)
    [[nodiscard]] frame_id_t find_victim_frame();

    size_t pool_size_;
    std::shared_ptr<DiskManager> disk_manager_;
    std::vector<Page> pages_;
    std::unique_ptr<LRUReplacer> replacer_;
    std::unordered_map<page_id_t, frame_id_t> page_table_;
    std::vector<frame_id_t> free_list_;
    std::mutex mutex_;
};

}  // namespace entropy
