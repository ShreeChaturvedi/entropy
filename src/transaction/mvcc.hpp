#pragma once

/**
 * @file mvcc.hpp
 * @brief Multi-Version Concurrency Control (MVCC) implementation
 *
 * MVCC allows multiple transactions to read data concurrently without
 * blocking, by maintaining version information for each tuple.
 *
 * Visibility Rules (Snapshot Isolation):
 * A transaction T can see a version V if:
 *   1. V was created by a committed transaction whose commit_ts <= T.start_ts
 *   2. V was not deleted, OR was deleted by a transaction whose commit_ts >
 * T.start_ts
 *   3. If V was created by T itself, it's always visible
 */

#include <atomic>
#include <limits>
#include <memory>

#include "common/types.hpp"

namespace entropy {

// Forward declarations
class Transaction;
class TransactionManager;

// ─────────────────────────────────────────────────────────────────────────────
// MVCC Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Maximum timestamp - indicates a version is still active/valid
static constexpr uint64_t TIMESTAMP_MAX = std::numeric_limits<uint64_t>::max();

/// Transaction ID indicating no transaction
static constexpr txn_id_t TXN_ID_NONE = INVALID_TXN_ID;

// ─────────────────────────────────────────────────────────────────────────────
// Version Information
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Version metadata stored with each tuple
 *
 * This 24-byte structure is stored as part of the tuple header to enable
 * MVCC visibility checks. It tracks which transaction created this version
 * and which (if any) deleted it.
 */
struct VersionInfo {
  /// Transaction that created this version
  txn_id_t created_by = TXN_ID_NONE;

  /// Transaction that deleted this version (TXN_ID_NONE if active)
  txn_id_t deleted_by = TXN_ID_NONE;

  /// Timestamp when this version became visible (creator's commit_ts)
  uint64_t begin_ts = 0;

  /// Timestamp when this version became invisible (deleter's commit_ts, or MAX
  /// if active)
  uint64_t end_ts = TIMESTAMP_MAX;

  /**
   * @brief Check if this version is deleted
   */
  [[nodiscard]] bool is_deleted() const noexcept {
    return deleted_by != TXN_ID_NONE || end_ts != TIMESTAMP_MAX;
  }

  /**
   * @brief Check if this version was created by a specific transaction
   */
  [[nodiscard]] bool created_by_txn(txn_id_t txn_id) const noexcept {
    return created_by == txn_id;
  }

  /**
   * @brief Check if this version was deleted by a specific transaction
   */
  [[nodiscard]] bool deleted_by_txn(txn_id_t txn_id) const noexcept {
    return deleted_by == txn_id;
  }
};

static_assert(sizeof(VersionInfo) == 32, "VersionInfo should be 32 bytes");

// ─────────────────────────────────────────────────────────────────────────────
// MVCC Manager
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Manages MVCC visibility and version lifecycle
 *
 * The MVCCManager provides:
 * - Visibility checking for snapshot isolation
 * - Timestamp generation for transactions
 * - Version creation/deletion tracking
 */
class MVCCManager {
public:
  MVCCManager();
  ~MVCCManager() = default;

  // Non-copyable
  MVCCManager(const MVCCManager &) = delete;
  MVCCManager &operator=(const MVCCManager &) = delete;

  // ─────────────────────────────────────────────────────────────────────────
  // Timestamp Generation
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Get the next timestamp for a new transaction
   */
  [[nodiscard]] uint64_t get_timestamp() noexcept;

  /**
   * @brief Get the current global timestamp (for debugging)
   */
  [[nodiscard]] uint64_t current_timestamp() const noexcept {
    return global_timestamp_.load();
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Visibility Checking
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Check if a version is visible to a transaction (Snapshot Isolation)
   *
   * A version is visible if:
   * 1. It was created by this transaction, OR
   * 2. It was created by a committed transaction before this transaction
   * started AND it's not deleted, or was deleted by a transaction after this
   * one started
   *
   * @param version The version to check
   * @param txn The transaction checking visibility
   * @return true if the version is visible to the transaction
   */
  [[nodiscard]] bool is_visible(const VersionInfo &version,
                                const Transaction *txn) const;

  /**
   * @brief Check if a version is visible (Read Committed isolation)
   *
   * For READ_COMMITTED, a version is visible if:
   * 1. It was created by this transaction, OR
   * 2. It was created by any committed transaction (regardless of when)
   *    AND it's not deleted, or deleted by an uncommitted transaction
   *
   * @param version The version to check
   * @param txn The transaction checking visibility
   * @param committed_txns Function to check if a transaction is committed
   * @return true if the version is visible
   */
  [[nodiscard]] bool
  is_visible_read_committed(const VersionInfo &version, const Transaction *txn,
                            std::function<bool(txn_id_t)> is_committed) const;

  // ─────────────────────────────────────────────────────────────────────────
  // Version Lifecycle
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Initialize version info for a newly inserted tuple
   * @param version The version info to initialize
   * @param txn The transaction creating the tuple
   */
  void init_version(VersionInfo &version, const Transaction *txn);

  /**
   * @brief Mark a version as deleted by a transaction
   * @param version The version to mark as deleted
   * @param txn The transaction deleting the tuple
   */
  void mark_deleted(VersionInfo &version, const Transaction *txn);

  /**
   * @brief Finalize version info when transaction commits
   * @param version The version info to finalize
   * @param commit_ts The commit timestamp
   */
  void finalize_commit(VersionInfo &version, uint64_t commit_ts);

  /**
   * @brief Rollback version creation (for aborted transactions)
   * @param version The version to invalidate
   */
  void rollback_version(VersionInfo &version);

private:
  /// Global monotonic timestamp counter
  std::atomic<uint64_t> global_timestamp_{1};
};

} // namespace entropy
