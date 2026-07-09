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
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
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
     * @brief Create a new WAL manager
     * @param log_file Path to the WAL file
     */
    explicit WALManager(const std::string& log_file);

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
     * @brief Durably sync the WAL file (fsync or test hook)
     */
    Status sync_file();

    std::string log_file_;
    std::fstream log_stream_;

    // Log buffer
    std::vector<char> buffer_;
    uint32_t buffer_offset_ = 0;

    // LSN tracking
    std::atomic<lsn_t> next_lsn_{1};
    std::atomic<lsn_t> flushed_lsn_{0};

    // Optional test hook replacing fsync
    std::function<Status()> sync_hook_;

    // Thread safety
    mutable std::mutex mutex_;
};

}  // namespace entropy
