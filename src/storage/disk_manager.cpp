/**
 * @file disk_manager.cpp
 * @brief Disk Manager implementation
 */

#include "storage/disk_manager.hpp"

#include <filesystem>

#include "common/logger.hpp"
#include "common/macros.hpp"

namespace entropy {

FileDiskManager::FileDiskManager(const std::string& db_file,
                                 bool create_if_missing, bool error_if_exists)
    : db_file_(db_file) {
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

    const auto offset = static_cast<std::streamoff>(page_id) *
                        static_cast<std::streamoff>(page_size());
    db_io_.seekg(offset, std::ios::beg);

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

    const auto offset = static_cast<std::streamoff>(page_id) *
                        static_cast<std::streamoff>(page_size());
    db_io_.seekp(offset, std::ios::beg);

    if (!db_io_) {
        return Status::IOError("Failed to seek to page");
    }

    db_io_.write(page_data, static_cast<std::streamsize>(page_size()));

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

    // Ignore ids that were never allocated.
    if (page_id < 0 || page_id >= num_pages_) {
        return;
    }

    free_list_.push_back(page_id);
}

void FileDiskManager::sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (db_io_.is_open()) {
        db_io_.flush();
    }
}

}  // namespace entropy
