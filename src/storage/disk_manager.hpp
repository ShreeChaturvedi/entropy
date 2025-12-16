#pragma once

/**
 * @file disk_manager.hpp
 * @brief Disk I/O operations
 */

#include <fstream>
#include <mutex>
#include <string>

#include "common/config.hpp"
#include "common/types.hpp"
#include "entropy/status.hpp"

namespace entropy {

/**
 * @brief Manages disk I/O operations
 *
 * DiskManager handles reading and writing pages to/from disk.
 * It manages the database file and provides atomic page operations.
 */
class DiskManager {
public:
    /**
     * @brief Construct a new Disk Manager
     * @param db_file Path to the database file
     */
    explicit DiskManager(const std::string& db_file);

    ~DiskManager();

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
    [[nodiscard]] Status read_page(page_id_t page_id, char* page_data);

    /**
     * @brief Write a page to disk
     * @param page_id The page to write
     * @param page_data Buffer to write from (must be page_size bytes)
     * @return Status of the operation
     */
    [[nodiscard]] Status write_page(page_id_t page_id, const char* page_data);

    /**
     * @brief Allocate a new page
     * @return The ID of the new page
     */
    [[nodiscard]] page_id_t allocate_page();

    /**
     * @brief Deallocate a page
     * @param page_id The page to deallocate
     */
    void deallocate_page(page_id_t page_id);

    /**
     * @brief Get the number of pages in the database
     */
    [[nodiscard]] page_id_t num_pages() const noexcept { return num_pages_; }

    /**
     * @brief Flush all writes to disk
     */
    void flush();

    /**
     * @brief Get the page size
     */
    [[nodiscard]] static constexpr size_t page_size() noexcept {
        return config::kDefaultPageSize;
    }

private:
    std::string db_file_;
    std::fstream db_io_;
    page_id_t num_pages_ = 0;
    mutable std::mutex mutex_;
};

}  // namespace entropy
