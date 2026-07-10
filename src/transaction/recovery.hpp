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
   * Flushes the WAL, then flushes all dirty pages, and only then appends a
   * CHECKPOINT record whose begin_lsn anchors the redo scan: every record
   * older than the anchor is durably reflected on disk pages at that point,
   * so recovery may safely skip it. Without a buffer pool (analysis-only
   * setups) no pages can be flushed and the record carries no anchor,
   * which makes recovery fall back to a full page-LSN-gated scan.
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
   * @brief Get count of redo operations actually applied to pages
   *
   * A record whose change is already reflected on its page (page LSN >= record
   * LSN) is skipped and not counted, so a second recover() over an already
   * recovered database reports zero.
   */
  [[nodiscard]] size_t redo_count() const noexcept { return redo_count_; }

  /**
   * @brief Get count of undo operations actually applied to pages
   */
  [[nodiscard]] size_t undo_count() const noexcept { return undo_count_; }

  /**
   * @brief Next transaction id to hand out after recovery
   *
   * Recovered as max(highest txn id seen in the log, checkpoint high-water) + 1
   * so post-restart transactions cannot alias recovered ones. The
   * TransactionManager consumes this on startup.
   */
  [[nodiscard]] txn_id_t next_txn_id() const noexcept {
    return recovered_next_txn_id_;
  }

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
   *
   * Applies inverse operations for all loser transactions in one merged
   * descending-LSN scan, flushes every page the undo mutated, and only then
   * appends (and flushes) an ABORT record per loser. That ordering makes the
   * ABORT a durable promise that its compensation is already on disk; a crash
   * at any point re-runs a state-checked, idempotent undo.
   *
   * @param records All log records from WAL (in LSN order)
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
   * @brief Undo a single log record (state-checked, idempotent)
   * @param record The log record to undo
   * @param[out] applied Set to true iff the page was actually mutated
   * @return Status indicating success or failure
   *
   * Each inverse op verifies the slot still reflects the logged operation
   * before touching it: undo-INSERT deletes only if the slot holds the
   * inserted bytes, undo-UPDATE restores only if the slot holds the
   * after-image, undo-DELETE re-inserts only into an empty slot. Slots
   * already undone (recovery re-run) or reused by a committed transaction
   * are left untouched.
   */
  Status undo_record(const LogRecord &record, bool *applied);

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

  // LSN at which the redo scan starts (checkpoint begin_lsn, else 0). Records
  // before this point are guaranteed durable on their pages.
  lsn_t redo_start_lsn_ = INVALID_LSN;

  // Next transaction id to hand out after recovery = max(seen, checkpoint) + 1.
  txn_id_t recovered_next_txn_id_ = 1;

  // Statistics for testing/debugging
  size_t redo_count_ = 0;
  size_t undo_count_ = 0;
};

} // namespace entropy
