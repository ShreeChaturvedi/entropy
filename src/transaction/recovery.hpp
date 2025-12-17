#pragma once

/**
 * @file recovery.hpp
 * @brief ARIES-style Crash Recovery Manager
 *
 * Implements the Analysis, Redo, Undo (ARU) recovery protocol:
 *
 * 1. Analysis Phase: Scan log from last checkpoint to build:
 *    - Active Transaction Table (ATT): transactions that were active at crash
 *    - Dirty Page Table (DPT): pages that may need redo
 *
 * 2. Redo Phase: Replay all operations from the log to bring the database
 *    to the state it was in at the time of the crash.
 *
 * 3. Undo Phase: Rollback all uncommitted transactions by traversing
 *    their prevLSN chains in reverse order.
 */

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/types.hpp"
#include "entropy/status.hpp"
#include "transaction/log_record.hpp"

namespace entropy {

// Forward declarations
class BufferPoolManager;
class WALManager;
class DiskManager;
class TableHeap;

/**
 * @brief ARIES-style Recovery Manager
 *
 * Provides crash recovery using Write-Ahead Logging. After a crash,
 * the recover() method is called to restore the database to a consistent
 * state with all committed transactions applied and all uncommitted
 * transactions rolled back.
 */
class RecoveryManager {
public:
  /**
   * @brief Construct a recovery manager
   * @param buffer_pool Buffer pool manager for page access
   * @param wal Write-ahead log manager
   * @param disk_manager Disk manager for direct page access during recovery
   */
  RecoveryManager(std::shared_ptr<BufferPoolManager> buffer_pool,
                  std::shared_ptr<WALManager> wal,
                  std::shared_ptr<DiskManager> disk_manager = nullptr);

  // Non-copyable
  RecoveryManager(const RecoveryManager &) = delete;
  RecoveryManager &operator=(const RecoveryManager &) = delete;

  /**
   * @brief Perform crash recovery
   *
   * This method should be called during database startup to recover
   * from any previous crash. It performs:
   * 1. Analysis: Scan log to identify active transactions and dirty pages
   * 2. Redo: Replay all logged operations
   * 3. Undo: Rollback uncommitted transactions
   *
   * @return Status indicating success or failure
   */
  [[nodiscard]] Status recover();

  /**
   * @brief Create a checkpoint
   *
   * A checkpoint reduces recovery time by recording:
   * - Currently active transactions
   * - Dirty pages in the buffer pool
   *
   * @param active_txn_ids List of currently active transaction IDs
   * @return Status indicating success or failure
   */
  [[nodiscard]] Status
  create_checkpoint(const std::vector<txn_id_t> &active_txn_ids);

  // ─────────────────────────────────────────────────────────────────────────
  // Accessors for testing
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Get the active transaction table after analysis
   */
  [[nodiscard]] const std::unordered_map<txn_id_t, lsn_t> &
  active_txn_table() const noexcept {
    return active_txn_table_;
  }

  /**
   * @brief Get the set of committed transactions found during analysis
   */
  [[nodiscard]] const std::unordered_set<txn_id_t> &
  committed_txns() const noexcept {
    return committed_txns_;
  }

  /**
   * @brief Get count of redo operations performed
   */
  [[nodiscard]] size_t redo_count() const noexcept { return redo_count_; }

  /**
   * @brief Get count of undo operations performed
   */
  [[nodiscard]] size_t undo_count() const noexcept { return undo_count_; }

private:
  // ─────────────────────────────────────────────────────────────────────────
  // ARIES Recovery Phases
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Analysis phase: Scan log to build ATT and identify committed txns
   * @param records All log records from WAL
   */
  void analysis_phase(const std::vector<LogRecord> &records);

  /**
   * @brief Redo phase: Replay all logged operations
   * @param records All log records from WAL
   * @return Status indicating success or failure
   */
  Status redo_phase(const std::vector<LogRecord> &records);

  /**
   * @brief Undo phase: Rollback uncommitted transactions
   * @param records All log records for building prevLSN chains
   * @return Status indicating success or failure
   */
  Status undo_phase(const std::vector<LogRecord> &records);

  /**
   * @brief Redo a single log record
   * @param record The log record to redo
   * @return Status indicating success or failure
   */
  Status redo_record(const LogRecord &record);

  /**
   * @brief Undo a single log record
   * @param record The log record to undo
   * @return Status indicating success or failure
   */
  Status undo_record(const LogRecord &record);

  /**
   * @brief Build a map from LSN to log record for efficient lookup
   * @param records All log records from WAL
   * @return Map from LSN to log record pointer
   */
  std::unordered_map<lsn_t, const LogRecord *>
  build_lsn_map(const std::vector<LogRecord> &records);

  // ─────────────────────────────────────────────────────────────────────────
  // Member Variables
  // ─────────────────────────────────────────────────────────────────────────

  std::shared_ptr<BufferPoolManager> buffer_pool_;
  std::shared_ptr<WALManager> wal_;
  std::shared_ptr<DiskManager> disk_manager_;

  // Active Transaction Table: txn_id -> last LSN for that transaction
  // Populated during analysis, contains uncommitted txns after analysis
  std::unordered_map<txn_id_t, lsn_t> active_txn_table_;

  // Set of committed transaction IDs
  std::unordered_set<txn_id_t> committed_txns_;

  // Statistics for testing/debugging
  size_t redo_count_ = 0;
  size_t undo_count_ = 0;
};

} // namespace entropy
