#pragma once

/**
 * @file disk_manager.hpp
 * @brief Disk I/O operations
 */

#include <fstream>
#include <mutex>
#include <string>
#include <vector>

#include "common/config.hpp"
#include "common/types.hpp"
#include "entropy/status.hpp"

namespace entropy {

/**
 * @brief Abstract disk I/O interface
 *
 * DiskManager is the page-level storage seam. It exposes reading, writing,
 * allocating and deallocating fixed-size pages. The default file-backed
 * implementation is FileDiskManager; the interface exists so alternative
 * backends (e.g. a fault-injecting simulator) can be swapped in without
 * touching the buffer pool or higher layers.
 */
class DiskManager {
public:
    virtual ~DiskManager() = default;

    // Non-copyable, non-movable
    DiskManager(const DiskManager&) = delete;
    DiskManager& operator=(const DiskManager&) = delete;
    DiskManager(DiskManager&&) = delete;
    DiskManager& operator=(DiskManager&&) = delete;

    /**
     * @brief Read a page from disk
     * @param page_id The page to read
     * @param page_data Buffer to read into (must be page_size bytes)
     * @return Status of the operation
     */
    [[nodiscard]] virtual Status read_page(page_id_t page_id, char* page_data) = 0;

    /**
     * @brief Write a page to disk
     * @param page_id The page to write
     * @param page_data Buffer to write from (must be page_size bytes)
     * @return Status of the operation
     */
    [[nodiscard]] virtual Status write_page(page_id_t page_id,
                                            const char* page_data) = 0;

    /**
     * @brief Allocate a new page
     * @return The ID of the new page
     */
    [[nodiscard]] virtual page_id_t allocate_page() = 0;

    /**
     * @brief Deallocate a page, making its id available for reuse
     * @param page_id The page to deallocate
     *
     * Out-of-range ids and double-frees are rejected (no-op). A reused id
     * reads back zeroed, like a fresh page.
     */
    virtual void deallocate_page(page_id_t page_id) = 0;

    /**
     * @brief Get the number of pages in the database
     */
    [[nodiscard]] virtual page_id_t num_pages() const noexcept = 0;

    /**
     * @brief Whether the underlying storage is open and usable
     */
    [[nodiscard]] virtual bool is_open() const noexcept = 0;

    /**
     * @brief Flush all pending writes to durable storage
     */
    virtual void sync() = 0;

    /**
     * @brief Get the page size
     */
    [[nodiscard]] static constexpr size_t page_size() noexcept {
        return config::kDefaultPageSize;
    }

protected:
    DiskManager() = default;
};

/**
 * @brief File-backed disk manager
 *
 * Stores pages in a single database file. Deallocated pages are zeroed on
 * disk and their ids held in an in-memory free list, reused by allocate_page
 * before the file is grown. The free list is not persisted: pages freed
 * before a restart are leaked (safe, but never reclaimed) until a durable
 * free-page map exists.
 */
class FileDiskManager : public DiskManager {
public:
    /**
     * @brief Construct a new File Disk Manager
     * @param db_file Path to the database file
     * @param create_if_missing Create the file when it does not exist
     * @param error_if_exists Fail to open when the file already exists
     * @param enable_checksums When true, every written page is stamped with a
     *        header checksum and every full read is verified against it, so a
     *        torn/partial page write is reported as a Corruption Status instead
     *        of silently returning corrupt bytes. Defaults to false: the
     *        checksum lives at the PageHeader offset that B+ tree pages reuse
     *        for parent_page_id, so it may only be enabled for a file that
     *        holds exclusively PageHeader-style pages. (See page.hpp.)
     *
     * On failure (file missing with create_if_missing=false, or file present
     * with error_if_exists=true) the manager is left closed; is_open() reports
     * false and no I/O is performed.
     */
    explicit FileDiskManager(const std::string& db_file,
                             bool create_if_missing = true,
                             bool error_if_exists = false,
                             bool enable_checksums = false);

    ~FileDiskManager() override;

    [[nodiscard]] Status read_page(page_id_t page_id, char* page_data) override;
    [[nodiscard]] Status write_page(page_id_t page_id,
                                    const char* page_data) override;
    [[nodiscard]] page_id_t allocate_page() override;
    void deallocate_page(page_id_t page_id) override;

    [[nodiscard]] page_id_t num_pages() const noexcept override {
        return num_pages_;
    }

    [[nodiscard]] bool is_open() const noexcept override {
        return db_io_.is_open();
    }

    void sync() override;

private:
    std::string db_file_;
    std::fstream db_io_;
    bool enable_checksums_ = false;
    page_id_t num_pages_ = 0;
    /// Freed page ids awaiting reuse. In-memory only — lost on restart, so
    /// pages freed before a crash/shutdown are leaked rather than reclaimed.
    std::vector<page_id_t> free_list_;
    mutable std::mutex mutex_;
};

}  // namespace entropy
