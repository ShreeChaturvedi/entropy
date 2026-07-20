/**
 * @file wal.cpp
 * @brief Write-Ahead Log implementation
 */

#include "transaction/wal.hpp"

#include <cerrno>
#include <cstring>
#include <filesystem>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

#include "common/logger.hpp"

namespace entropy {

WALManager::WALManager(const std::string& log_file) : log_file_(log_file) {
    buffer_.resize(WAL_BUFFER_SIZE);

    // Open log file for reading and writing
    // Create if it doesn't exist
    log_stream_.open(log_file_, std::ios::in | std::ios::out | std::ios::binary);
    if (!log_stream_.is_open()) {
        // File doesn't exist, create it
        log_stream_.clear();
        log_stream_.open(log_file_, std::ios::out | std::ios::binary);
        log_stream_.close();
        log_stream_.open(log_file_, std::ios::in | std::ios::out | std::ios::binary);
    }

    if (!log_stream_.is_open()) {
        LOG_ERROR("Failed to open WAL file: {}", log_file_);
        throw std::runtime_error("Failed to open WAL file: " + log_file_);
    }

    // Seek to end for appending
    log_stream_.seekp(0, std::ios::end);

    // Determine next LSN based on existing records
    auto existing_records = read_log();
    if (!existing_records.empty()) {
        next_lsn_ = existing_records.back().lsn() + 1;
        flushed_lsn_ = existing_records.back().lsn();
    }

    LOG_INFO("WAL opened: {} (next_lsn={}, flushed_lsn={})", log_file_, next_lsn_.load(),
             flushed_lsn_.load());
}

WALManager::~WALManager() {
    // Flush any remaining buffered records
    if (buffer_offset_ > 0) {
        (void)flush();
    }
    log_stream_.close();
}

void WALManager::set_sync_hook_for_testing(std::function<Status()> hook) {
    std::lock_guard<std::mutex> lock(mutex_);
    sync_hook_ = std::move(hook);
}

Status WALManager::sync_file() {
    // Ensure iostream buffers are pushed to the OS before durable sync.
    log_stream_.flush();
    if (log_stream_.fail()) {
        return Status::IOError("Failed to flush WAL stream before fsync");
    }

    if (sync_hook_) {
        return sync_hook_();
    }

#ifdef _WIN32
    // Re-open by path to obtain a HANDLE and flush its buffers to disk. This
    // mirrors the POSIX path below without relying on the stream's descriptor.
    const HANDLE handle =
        ::CreateFileA(log_file_.c_str(), GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
                      nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        return Status::IOError("Failed to open WAL file for flush: " + log_file_);
    }

    const BOOL ok = ::FlushFileBuffers(handle);
    const DWORD err = ::GetLastError();
    ::CloseHandle(handle);

    if (!ok) {
        return Status::IOError("FlushFileBuffers failed on WAL file: " + log_file_ +
                               " (error=" + std::to_string(err) + ")");
    }

    return Status::Ok();
#else
    // Re-open by path to obtain a file descriptor for fsync. This is portable
    // across standard library implementations that do not expose fileno().
    const int fd = ::open(log_file_.c_str(), O_RDWR);
    if (fd < 0) {
        return Status::IOError("Failed to open WAL file for fsync: " + log_file_);
    }

    const int rc = ::fsync(fd);
    const int err = errno;
    ::close(fd);

    if (rc != 0) {
        return Status::IOError("fsync failed on WAL file: " + log_file_ +
                               " (errno=" + std::to_string(err) + ")");
    }

    return Status::Ok();
#endif
}

lsn_t WALManager::append_log(LogRecord& record) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Assign LSN
    lsn_t lsn = next_lsn_.fetch_add(1);
    record.set_lsn(lsn);

    // Serialize the record
    std::vector<char> serialized = record.serialize();
    uint32_t record_size = static_cast<uint32_t>(serialized.size());

    // Check if we need to flush first
    if (buffer_offset_ + record_size > WAL_BUFFER_SIZE) {
        Status status = flush_internal();
        if (!status.ok()) {
            LOG_ERROR("Failed to flush WAL buffer: {}", status.message());
            // Roll back LSN assignment so callers see a hard failure.
            next_lsn_.store(lsn);
            record.set_lsn(INVALID_LSN);
            return INVALID_LSN;
        }
    }

    // If single record is larger than buffer, write directly to file
    if (record_size > WAL_BUFFER_SIZE) {
        // Flush any existing buffer content first
        if (buffer_offset_ > 0) {
            Status status = flush_internal();
            if (!status.ok()) {
                LOG_ERROR("Failed to flush WAL buffer before large write: {}", status.message());
                next_lsn_.store(lsn);
                record.set_lsn(INVALID_LSN);
                return INVALID_LSN;
            }
        }

        // Write large record directly
        log_stream_.write(serialized.data(), static_cast<std::streamsize>(record_size));
        if (log_stream_.fail()) {
            LOG_ERROR("Failed to write large WAL record");
            next_lsn_.store(lsn);
            record.set_lsn(INVALID_LSN);
            return INVALID_LSN;
        }

        Status status = sync_file();
        if (!status.ok()) {
            LOG_ERROR("Failed to sync large WAL record: {}", status.message());
            next_lsn_.store(lsn);
            record.set_lsn(INVALID_LSN);
            return INVALID_LSN;
        }
        flushed_lsn_.store(lsn);
    } else {
        // Copy to buffer
        std::memcpy(buffer_.data() + buffer_offset_, serialized.data(), record_size);
        buffer_offset_ += record_size;
    }

    return lsn;
}

Status WALManager::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    return flush_internal();
}

Status WALManager::flush_internal() {
    if (buffer_offset_ == 0) {
        return Status::Ok();
    }

    const auto write_pos = log_stream_.tellp();

    // Write buffer to file
    log_stream_.write(buffer_.data(), static_cast<std::streamsize>(buffer_offset_));

    if (log_stream_.fail()) {
        log_stream_.clear();
        if (write_pos != std::streampos(-1)) {
            log_stream_.seekp(write_pos);
        }
        return Status::IOError("Failed to write to WAL file");
    }

    Status sync_status = sync_file();
    if (!sync_status.ok()) {
        // Keep buffer contents and rewind so a retry rewrites the same bytes.
        log_stream_.clear();
        if (write_pos != std::streampos(-1)) {
            log_stream_.seekp(write_pos);
        }
        return sync_status;
    }

    // Update flushed LSN
    flushed_lsn_.store(next_lsn_.load() - 1);

    // Reset buffer
    buffer_offset_ = 0;

    return Status::Ok();
}

Status WALManager::flush_to_lsn(lsn_t lsn) {
    if (lsn <= flushed_lsn_.load()) {
        // Already flushed
        return Status::Ok();
    }

    return flush();
}

std::vector<LogRecord> WALManager::read_log() {
    std::vector<LogRecord> records;

    // Create a separate stream for reading to avoid messing with write position
    std::ifstream read_stream(log_file_, std::ios::binary);
    if (!read_stream.is_open()) {
        return records;
    }

    // Read all records
    while (read_stream.good()) {
        // Read header first to get size
        LogRecordHeader header;
        read_stream.read(reinterpret_cast<char*>(&header), LOG_RECORD_HEADER_SIZE);

        if (read_stream.gcount() < static_cast<std::streamsize>(LOG_RECORD_HEADER_SIZE)) {
            break;  // End of file or incomplete record
        }

        if (header.size < LOG_RECORD_HEADER_SIZE || header.size > WAL_MAX_RECORD_SIZE) {
            LOG_WARN("Invalid log record size: {}", header.size);
            break;
        }

        // Read full record
        std::vector<char> buffer(header.size);
        std::memcpy(buffer.data(), &header, LOG_RECORD_HEADER_SIZE);

        if (header.size > LOG_RECORD_HEADER_SIZE) {
            read_stream.read(buffer.data() + LOG_RECORD_HEADER_SIZE,
                             static_cast<std::streamsize>(header.size - LOG_RECORD_HEADER_SIZE));

            if (read_stream.gcount() <
                static_cast<std::streamsize>(header.size - LOG_RECORD_HEADER_SIZE)) {
                LOG_WARN("Incomplete log record, truncated WAL");
                break;
            }
        }

        // Deserialize and add to results
        LogRecord record;
        if (!LogRecord::try_deserialize(buffer.data(), header.size, record)) {
            LOG_WARN("Corrupt log record at LSN {}, stopping WAL read", header.lsn);
            break;
        }
        records.push_back(std::move(record));
    }

    return records;
}

}  // namespace entropy
