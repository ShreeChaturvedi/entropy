/**
 * @file disk_manager.cpp
 * @brief Disk Manager implementation
 */

#include "storage/disk_manager.hpp"

#include <algorithm>
#include <array>
#include <filesystem>

#include "common/logger.hpp"
#include "common/macros.hpp"
#include "storage/page.hpp"

namespace entropy {
namespace {

// Byte offset of a page within the database file.
[[nodiscard]] std::streamoff page_offset(page_id_t page_id) {
    return static_cast<std::streamoff>(page_id) *
           static_cast<std::streamoff>(DiskManager::page_size());
}

}  // namespace

FileDiskManager::FileDiskManager(const std::string& db_file,
                                 bool create_if_missing, bool error_if_exists,
                                 bool enable_checksums)
    : db_file_(db_file), enable_checksums_(enable_checksums) {
    const bool exists = std::filesystem::exists(db_file_);

    if (exists && error_if_exists) {
        LOG_ERROR("Database file already exists: {}", db_file_);
        return;  // Leave closed; is_open() reports false.
    }
    if (!exists && !create_if_missing) {
        LOG_ERROR("Database file does not exist: {}", db_file_);
        return;  // Leave closed; is_open() reports false.
    }

    // Open file for reading and writing, create if doesn't exist
    db_io_.open(db_file_, std::ios::binary | std::ios::in | std::ios::out);

    if (!db_io_.is_open()) {
        // File doesn't exist, create it
        db_io_.clear();
        db_io_.open(db_file_, std::ios::binary | std::ios::trunc | std::ios::out);
        db_io_.close();
        db_io_.open(db_file_, std::ios::binary | std::ios::in | std::ios::out);

        if (!db_io_.is_open()) {
            LOG_ERROR("Failed to create database file: {}", db_file_);
            return;
        }
    }

    // Determine number of pages
    db_io_.seekg(0, std::ios::end);
    auto file_size = db_io_.tellg();
    const auto page_size_off = static_cast<std::streamoff>(page_size());
    if (file_size < 0) {
        num_pages_ = 0;
    } else {
        num_pages_ = static_cast<page_id_t>(file_size / page_size_off);
    }

    LOG_INFO("Opened database file: {} with {} pages", db_file_, num_pages_);
}

FileDiskManager::~FileDiskManager() {
    if (db_io_.is_open()) {
        db_io_.close();
    }
}

Status FileDiskManager::read_page(page_id_t page_id, char* page_data) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (page_id < 0) {
        return Status::InvalidArgument("Invalid page ID");
    }

    // A prior short/beyond-EOF read leaves failbit set; clear so this I/O
    // is not spuriously rejected (and so we do not latch all later I/O off).
    db_io_.clear();

    db_io_.seekg(page_offset(page_id), std::ios::beg);

    if (!db_io_) {
        return Status::IOError("Failed to seek to page");
    }

    db_io_.read(page_data, static_cast<std::streamsize>(page_size()));

    if (db_io_.bad()) {
        return Status::IOError("Failed to read page");
    }

    // Handle reading beyond file (return zeroed page). Short reads set
    // failbit; clear it so subsequent operations on this fstream succeed.
    const auto bytes_read = db_io_.gcount();
    if (bytes_read < 0) {
        return Status::IOError("Failed to read page");
    }
    const auto bytes_read_size = static_cast<size_t>(bytes_read);
    if (bytes_read_size < page_size()) {
        std::memset(page_data + bytes_read_size, 0,
                    page_size() - bytes_read_size);
        db_io_.clear();
        // A short (beyond-EOF) read is a fresh, zero-filled page, not stored
        // data, so it carries no checksum to verify.
        return Status::Ok();
    }

    // Verify the integrity stamp on a fully-read page: a torn/partial write is
    // caught here rather than silently handed back as corrupt data.
    if (enable_checksums_ && !verify_page_checksum(page_data)) {
        return Status::Corruption("torn/corrupt page: checksum mismatch");
    }

    return Status::Ok();
}

Status FileDiskManager::write_page(page_id_t page_id, const char* page_data) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (page_id < 0) {
        return Status::InvalidArgument("Invalid page ID");
    }

    // Clear any leftover failbit/eofbit from a prior short read.
    db_io_.clear();

    db_io_.seekp(page_offset(page_id), std::ios::beg);

    if (!db_io_) {
        return Status::IOError("Failed to seek to page");
    }

    // Stamp the integrity checksum into a private copy so the caller's buffer is
    // never mutated. The on-disk image then carries a checksum that read_page
    // re-verifies to catch a later torn write.
    const char* out = page_data;
    std::array<char, config::kDefaultPageSize> stamped;
    if (enable_checksums_) {
        std::memcpy(stamped.data(), page_data, stamped.size());
        stamp_page_checksum(stamped.data());
        out = stamped.data();
    }

    db_io_.write(out, static_cast<std::streamsize>(page_size()));

    if (db_io_.bad()) {
        return Status::IOError("Failed to write page");
    }

    db_io_.flush();

    // Update page count if necessary
    if (page_id >= num_pages_) {
        num_pages_ = page_id + 1;
    }

    return Status::Ok();
}

page_id_t FileDiskManager::allocate_page() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Reuse a deallocated page id before growing the file.
    if (!free_list_.empty()) {
        page_id_t page_id = free_list_.back();
        free_list_.pop_back();
        return page_id;
    }

    return num_pages_++;
}

void FileDiskManager::deallocate_page(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Reject ids that were never allocated.
    if (page_id < 0 || page_id >= num_pages_) {
        LOG_WARN("Ignoring deallocation of out-of-range page {}", page_id);
        return;
    }

    // Reject double-frees: the id is already available for reuse.
    if (std::find(free_list_.begin(), free_list_.end(), page_id) !=
        free_list_.end()) {
        LOG_WARN("Ignoring double deallocation of page {}", page_id);
        return;
    }

    // Zero the page on disk so a reused id reads back like a fresh page.
    db_io_.clear();
    db_io_.seekp(page_offset(page_id), std::ios::beg);
    const std::vector<char> zeros(page_size(), 0);
    db_io_.write(zeros.data(), static_cast<std::streamsize>(page_size()));
    if (db_io_.bad()) {
        // Leak the page rather than risk resurfacing stale bytes on reuse.
        LOG_ERROR("Failed to zero deallocated page {}; id not reused", page_id);
        return;
    }
    db_io_.flush();

    free_list_.push_back(page_id);
}

void FileDiskManager::sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (db_io_.is_open()) {
        db_io_.flush();
    }
}

}  // namespace entropy
