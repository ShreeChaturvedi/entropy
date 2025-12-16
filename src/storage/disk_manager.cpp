/**
 * @file disk_manager.cpp
 * @brief Disk Manager implementation
 */

#include "storage/disk_manager.hpp"

#include <filesystem>

#include "common/logger.hpp"
#include "common/macros.hpp"

namespace entropy {

DiskManager::DiskManager(const std::string& db_file) : db_file_(db_file) {
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
    num_pages_ = static_cast<page_id_t>(file_size / page_size());

    LOG_INFO("Opened database file: {} with {} pages", db_file_, num_pages_);
}

DiskManager::~DiskManager() {
    if (db_io_.is_open()) {
        db_io_.close();
    }
}

Status DiskManager::read_page(page_id_t page_id, char* page_data) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (page_id < 0) {
        return Status::InvalidArgument("Invalid page ID");
    }

    auto offset = static_cast<std::streamoff>(page_id) * page_size();
    db_io_.seekg(offset);

    if (!db_io_) {
        return Status::IOError("Failed to seek to page");
    }

    db_io_.read(page_data, static_cast<std::streamsize>(page_size()));

    if (db_io_.bad()) {
        return Status::IOError("Failed to read page");
    }

    // Handle reading beyond file (return zeroed page)
    auto bytes_read = db_io_.gcount();
    if (bytes_read < static_cast<std::streamsize>(page_size())) {
        std::memset(page_data + bytes_read, 0, page_size() - bytes_read);
    }

    return Status::Ok();
}

Status DiskManager::write_page(page_id_t page_id, const char* page_data) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (page_id < 0) {
        return Status::InvalidArgument("Invalid page ID");
    }

    auto offset = static_cast<std::streamoff>(page_id) * page_size();
    db_io_.seekp(offset);

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

page_id_t DiskManager::allocate_page() {
    std::lock_guard<std::mutex> lock(mutex_);
    return num_pages_++;
}

void DiskManager::deallocate_page([[maybe_unused]] page_id_t page_id) {
    // TODO: Implement free page tracking
    // For now, pages are not reused
}

void DiskManager::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (db_io_.is_open()) {
        db_io_.flush();
    }
}

}  // namespace entropy
