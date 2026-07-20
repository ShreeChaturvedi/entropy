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

// ─────────────────────────────────────────────────────────────────────────────
// FileLogStore
// ─────────────────────────────────────────────────────────────────────────────

FileLogStore::FileLogStore(std::string path) : path_(std::move(path)) {
    // Open the file for reading and writing, creating it if it does not exist.
    stream_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!stream_.is_open()) {
        stream_.clear();
        stream_.open(path_, std::ios::out | std::ios::binary);
        stream_.close();
        stream_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    }

    if (!stream_.is_open()) {
        LOG_ERROR("Failed to open WAL file: {}", path_);
        throw std::runtime_error("Failed to open WAL file: " + path_);
    }

    // Determine current size and seek to end for appending.
    stream_.seekp(0, std::ios::end);
    const auto end_pos = stream_.tellp();
    size_ = (end_pos > 0) ? static_cast<uint64_t>(end_pos) : 0;
}

FileLogStore::~FileLogStore() {
    if (stream_.is_open()) {
        stream_.close();
    }
}

void FileLogStore::set_sync_hook_for_testing(std::function<Status()> hook) {
    std::lock_guard<std::mutex> lock(mutex_);
    sync_hook_ = std::move(hook);
}

Status FileLogStore::append(std::span<const char> data) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (data.empty()) {
        return Status::Ok();
    }

    const auto write_pos = stream_.tellp();
    stream_.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (stream_.fail()) {
        // Leave the stream unchanged so a retry re-appends the same bytes.
        stream_.clear();
        if (write_pos != std::streampos(-1)) {
            stream_.seekp(write_pos);
        }
        return Status::IOError("Failed to write to WAL file");
    }

    size_ += data.size();
    return Status::Ok();
}

Status FileLogStore::sync() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Ensure iostream buffers are pushed to the OS before durable sync.
    stream_.flush();
    if (stream_.fail()) {
        return Status::IOError("Failed to flush WAL stream before fsync");
    }

    if (sync_hook_) {
        return sync_hook_();
    }

#ifdef _WIN32
    // Re-open by path to obtain a HANDLE and flush its buffers to disk. This
    // mirrors the POSIX path below without relying on the stream's descriptor.
    const HANDLE handle =
        ::CreateFileA(path_.c_str(), GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
                      nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        return Status::IOError("Failed to open WAL file for flush: " + path_);
    }

    const BOOL ok = ::FlushFileBuffers(handle);
    const DWORD err = ::GetLastError();
    ::CloseHandle(handle);

    if (!ok) {
        return Status::IOError("FlushFileBuffers failed on WAL file: " + path_ +
                               " (error=" + std::to_string(err) + ")");
    }

    return Status::Ok();
#else
    // Re-open by path to obtain a file descriptor for fsync. This is portable
    // across standard library implementations that do not expose fileno().
    const int fd = ::open(path_.c_str(), O_RDWR);
    if (fd < 0) {
        return Status::IOError("Failed to open WAL file for fsync: " + path_);
    }

    const int rc = ::fsync(fd);
    const int err = errno;
    ::close(fd);

    if (rc != 0) {
        return Status::IOError("fsync failed on WAL file: " + path_ +
                               " (errno=" + std::to_string(err) + ")");
    }

    return Status::Ok();
#endif
}

std::vector<char> FileLogStore::read_all() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Push any pending writes so a fresh reader observes the latest bytes.
    stream_.flush();

    std::ifstream read_stream(path_, std::ios::binary);
    if (!read_stream.is_open()) {
        return {};
    }

    read_stream.seekg(0, std::ios::end);
    const auto end_pos = read_stream.tellg();
    if (end_pos <= 0) {
        return {};
    }
    read_stream.seekg(0, std::ios::beg);

    std::vector<char> bytes(static_cast<size_t>(end_pos));
    read_stream.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    bytes.resize(static_cast<size_t>(read_stream.gcount()));
    return bytes;
}

// ─────────────────────────────────────────────────────────────────────────────
// WALManager
// ─────────────────────────────────────────────────────────────────────────────

WALManager::WALManager(const std::string& log_file)
    : log_file_(log_file),
      log_store_(std::make_shared<FileLogStore>(log_file)) {
    init_after_open();
}

WALManager::WALManager(std::shared_ptr<LogStore> log_store)
    : log_store_(std::move(log_store)) {
    init_after_open();
}

void WALManager::init_after_open() {
    buffer_.resize(WAL_BUFFER_SIZE);

    // Determine next LSN based on existing records.
    auto existing_records = read_log();
    if (!existing_records.empty()) {
        next_lsn_ = existing_records.back().lsn() + 1;
        flushed_lsn_ = existing_records.back().lsn();
    }

    LOG_INFO("WAL opened: {} (next_lsn={}, flushed_lsn={})", log_file_, next_lsn_.load(),
             flushed_lsn_.load());
}

WALManager::~WALManager() {
    // Flush any buffered or appended-but-unsynced records.
    (void)flush();
}

void WALManager::set_sync_hook_for_testing(std::function<Status()> hook) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (log_store_) {
        log_store_->set_sync_hook_for_testing(std::move(hook));
    }
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

    // If single record is larger than buffer, write directly to the store
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
        Status write_status =
            log_store_->append(std::span<const char>(serialized.data(), record_size));
        if (!write_status.ok()) {
            LOG_ERROR("Failed to write large WAL record: {}", write_status.message());
            next_lsn_.store(lsn);
            record.set_lsn(INVALID_LSN);
            return INVALID_LSN;
        }

        Status status = log_store_->sync();
        if (!status.ok()) {
            LOG_ERROR("Failed to sync large WAL record: {}", status.message());
            next_lsn_.store(lsn);
            record.set_lsn(INVALID_LSN);
            return INVALID_LSN;
        }
        // Only track the LSN once durable: on sync failure it is rolled back
        // and reused, so it must not linger as an appended watermark.
        appended_max_lsn_ = lsn;
        flushed_lsn_.store(lsn);
    } else {
        // Copy to buffer
        std::memcpy(buffer_.data() + buffer_offset_, serialized.data(), record_size);
        buffer_offset_ += record_size;
        // This record is now the highest LSN held (but not yet flushed) in the
        // buffer. flush_internal uses this to advance flushed_lsn_ truthfully.
        buffered_max_lsn_ = lsn;
    }

    return lsn;
}

Status WALManager::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    return flush_internal();
}

Status WALManager::flush_internal() {
    // Stage 1: hand buffered bytes to the store exactly once. append() is
    // atomic on write failure (the store is left unchanged), so on error the
    // kept buffer is safely re-appended by a retry.
    if (buffer_offset_ > 0) {
        Status write_status =
            log_store_->append(std::span<const char>(buffer_.data(), buffer_offset_));
        if (!write_status.ok()) {
            return write_status;
        }

        // Track the highest LSN handed to the store. Using next_lsn_ - 1 here
        // would wrongly cover a record that append_log has assigned an LSN to
        // but not yet copied into the buffer (e.g. the record that triggered
        // an overflow flush), silently skipping its later fsync.
        if (buffered_max_lsn_ > appended_max_lsn_) {
            appended_max_lsn_ = buffered_max_lsn_;
        }
        buffer_offset_ = 0;
        buffered_max_lsn_ = INVALID_LSN;
    }

    // Stage 2: make appended bytes durable. On sync failure the bytes stay in
    // the store and a retry only re-drives the sync — never a second append —
    // so a transient sync failure cannot duplicate records or LSNs in the log.
    if (appended_max_lsn_ > flushed_lsn_.load()) {
        Status sync_status = log_store_->sync();
        if (!sync_status.ok()) {
            return sync_status;
        }
        flushed_lsn_.store(appended_max_lsn_);
    }

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
    if (!log_store_) {
        return records;
    }

    const std::vector<char> bytes = log_store_->read_all();
    size_t pos = 0;

    while (pos + LOG_RECORD_HEADER_SIZE <= bytes.size()) {
        LogRecordHeader header;
        std::memcpy(&header, bytes.data() + pos, LOG_RECORD_HEADER_SIZE);

        if (header.size < LOG_RECORD_HEADER_SIZE || header.size > WAL_MAX_RECORD_SIZE) {
            LOG_WARN("Invalid log record size: {}", header.size);
            break;
        }

        if (pos + header.size > bytes.size()) {
            LOG_WARN("Incomplete log record, truncated WAL");
            break;
        }

        LogRecord record;
        if (!LogRecord::try_deserialize(bytes.data() + pos, header.size, record)) {
            LOG_WARN("Corrupt log record at LSN {}, stopping WAL read", header.lsn);
            break;
        }
        records.push_back(std::move(record));
        pos += header.size;
    }

    return records;
}

}  // namespace entropy
