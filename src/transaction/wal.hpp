#pragma once

/**
 * @file wal.hpp
 * @brief Write-Ahead Log Manager
 *
 * The WAL ensures durability and atomicity by writing log records
 * before modifying data pages. Key principles:
 *
 * 1. Write-Ahead Logging: Log record written before data change
 * 2. Force on Commit: Log records flushed to disk before commit returns
 * 3. Steal Policy: Dirty pages can be written before commit (requires undo)
 *
 * The log buffer accumulates records and is flushed either:
 * - When a transaction commits (force)
 * - When the buffer becomes full
 * - Periodically via background thread (not implemented yet)
 */

#include <atomic>
#include <cstdint>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "common/config.hpp"
#include "common/types.hpp"
#include "entropy/status.hpp"
#include "transaction/log_record.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// WAL Configuration
// ─────────────────────────────────────────────────────────────────────────────

static constexpr uint32_t WAL_BUFFER_SIZE = 64 * 1024;  // 64KB log buffer

/// Maximum accepted on-disk log record size (guards against corrupt headers).
static constexpr uint32_t WAL_MAX_RECORD_SIZE = 16 * 1024 * 1024;  // 16MB

// ─────────────────────────────────────────────────────────────────────────────
// LogStore — durable byte-stream seam
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Append-only durable byte-stream the WAL is written through
 *
 * Abstracting the underlying medium lets the crash simulator supply a
 * fault-injecting store (drop/reorder/tear at sync boundaries) without
 * touching WAL record logic. The default production implementation is
 * FileLogStore (fstream + fsync).
 *
 * Thread safety: implementations are self-synchronizing; WALManager also
 * serializes its own access.
 */
class LogStore {
public:
    virtual ~LogStore() = default;

    /// Append raw bytes to the end of the stream (atomic: on write failure the
    /// stream is left unchanged).
    [[nodiscard]] virtual Status append(std::span<const char> data) = 0;

    /// Durably persist all appended bytes (fsync, or the installed test hook).
    [[nodiscard]] virtual Status sync() = 0;

    /// Read back the entire stream contents in append order.
    [[nodiscard]] virtual std::vector<char> read_all() = 0;

    /// Current number of bytes held by the stream.
    [[nodiscard]] virtual uint64_t size() const = 0;

    /// Install a callback used instead of the real durable sync (tests only).
    virtual void set_sync_hook_for_testing(std::function<Status()> hook) = 0;
};

/**
 * @brief File-backed LogStore wrapping an fstream plus a portable fsync
 *
 * Preserves the Windows-portable durable-sync path (FlushFileBuffers) and the
 * POSIX fsync path, and hosts the test sync hook.
 */
class FileLogStore : public LogStore {
public:
    explicit FileLogStore(std::string path);
    ~FileLogStore() override;

    // Non-copyable
    FileLogStore(const FileLogStore&) = delete;
    FileLogStore& operator=(const FileLogStore&) = delete;

    [[nodiscard]] Status append(std::span<const char> data) override;
    [[nodiscard]] Status sync() override;
    [[nodiscard]] std::vector<char> read_all() override;
    [[nodiscard]] uint64_t size() const override { return size_; }
    void set_sync_hook_for_testing(std::function<Status()> hook) override;

    /// Path of the backing file.
    [[nodiscard]] const std::string& path() const noexcept { return path_; }

private:
    std::string path_;
    std::fstream stream_;
    uint64_t size_ = 0;
    std::function<Status()> sync_hook_;
    mutable std::mutex mutex_;
};

// ─────────────────────────────────────────────────────────────────────────────
// WAL Manager
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Manages the Write-Ahead Log
 *
 * Thread safety: All public methods are thread-safe.
 */
class WALManager {
public:
    /**
     * @brief Create a new WAL manager backed by a file
     * @param log_file Path to the WAL file
     *
     * Builds a FileLogStore internally so existing callers compile unchanged.
     */
    explicit WALManager(const std::string& log_file);

    /**
     * @brief Create a WAL manager writing through a caller-supplied LogStore
     * @param log_store Durable byte-stream (e.g. FileLogStore or a simulator)
     */
    explicit WALManager(std::shared_ptr<LogStore> log_store);

    /**
     * @brief Destructor - flushes any remaining log records
     */
    ~WALManager();

    // Non-copyable
    WALManager(const WALManager&) = delete;
    WALManager& operator=(const WALManager&) = delete;

    // ─────────────────────────────────────────────────────────────────────────
    // Log Operations
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Append a log record to the WAL
     * @param record The log record to append
     * @return The LSN assigned to this record, or INVALID_LSN on I/O failure
     *
     * This method:
     * 1. Assigns an LSN to the record
     * 2. Serializes the record to the log buffer
     * 3. Flushes if buffer is full
     */
    [[nodiscard]] lsn_t append_log(LogRecord& record);

    /**
     * @brief Flush all buffered log records to durable storage
     * @return Status indicating success or failure
     *
     * Writes buffered records, flushes the stream, then fsyncs the file
     * (or invokes the test sync hook when installed).
     */
    [[nodiscard]] Status flush();

    /**
     * @brief Flush log records up to and including the given LSN
     * @param lsn The LSN to flush up to
     * @return Status indicating success or failure
     */
    [[nodiscard]] Status flush_to_lsn(lsn_t lsn);

    /**
     * @brief Install a sync hook used instead of fsync (tests only)
     *
     * When set, flush durability is delegated to this callback so tests can
     * assert that a durable sync was requested without relying on OS fsync.
     */
    void set_sync_hook_for_testing(std::function<Status()> hook);

    // ─────────────────────────────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Get the last flushed LSN
     * All log records with LSN <= flushed_lsn are guaranteed to be on disk.
     */
    [[nodiscard]] lsn_t flushed_lsn() const noexcept { return flushed_lsn_.load(); }

    /**
     * @brief Get the next LSN that will be assigned
     */
    [[nodiscard]] lsn_t next_lsn() const noexcept { return next_lsn_.load(); }

    /**
     * @brief Get current buffer offset (for debugging)
     */
    [[nodiscard]] uint32_t buffer_offset() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return buffer_offset_;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Recovery Support
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Read all log records from the WAL file
     * @return Vector of log records in order
     *
     * Used during crash recovery.
     */
    [[nodiscard]] std::vector<LogRecord> read_log();

    /**
     * @brief Get the path to the WAL file
     */
    [[nodiscard]] const std::string& log_file() const noexcept { return log_file_; }

private:
    /**
     * @brief Internal flush without locking
     */
    Status flush_internal();

    /**
     * @brief Shared post-open initialization (buffer + LSN recovery)
     */
    void init_after_open();

    std::string log_file_;
    std::shared_ptr<LogStore> log_store_;

    // Log buffer
    std::vector<char> buffer_;
    uint32_t buffer_offset_ = 0;

    // Highest LSN of a record currently sitting in buffer_ (INVALID when
    // empty). flush_internal advances the durability watermark only through
    // this value, so a record that has merely been assigned an LSN (but not
    // yet copied into the buffer) never counts as durable.
    lsn_t buffered_max_lsn_ = INVALID_LSN;

    // Highest LSN whose bytes have been handed to the log store. Bytes are
    // appended to the store exactly once; if the subsequent sync fails, a
    // retry only re-drives the sync (never re-appends), so a transient sync
    // failure cannot duplicate records in the log.
    lsn_t appended_max_lsn_ = INVALID_LSN;

    // LSN tracking
    std::atomic<lsn_t> next_lsn_{1};
    std::atomic<lsn_t> flushed_lsn_{0};

    // Thread safety
    mutable std::mutex mutex_;
};

}  // namespace entropy
