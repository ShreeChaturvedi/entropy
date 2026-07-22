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

/// Commit timestamp reserved for synthetic "pre-history" base versions — the
/// stand-in committed version materialized for a row that predates its chain
/// (e.g. loaded from disk before any live transaction). It sits strictly below
/// the first timestamp @ref MVCCManager::get_timestamp ever hands out, so it
/// can never collide with a real commit_ts while still comparing as committed
/// and visible to every snapshot.
static constexpr uint64_t TIMESTAMP_PREHISTORY = 1;

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
   * @brief Advance and return the single monotonic logical clock
   *
   * This is the one authoritative timestamp source for the engine. Both
   * transaction start timestamps (snapshots) and commit timestamps are drawn
   * from it, so all MVCC visibility comparisons are well-ordered. Each call
   * returns a strictly larger value than the previous.
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
   * A version is visible iff its creation is visible and its deletion is not:
   *   is_visible == is_created_visible && !is_delete_visible
   *
   * All comparisons use the single logical clock (@ref get_timestamp): a
   * version's begin_ts/end_ts are stamped from a committer's commit_ts, and a
   * reader's snapshot is its start_ts, both drawn from the same monotonic
   * counter. This keeps snapshot ordering well-defined.
   *
   * @param version The version to check
   * @param txn The transaction checking visibility (nullptr = latest committed)
   * @return true if the version is visible to the transaction
   */
  [[nodiscard]] bool is_visible(const VersionInfo &version,
                                const Transaction *txn) const;

  /**
   * @brief Whether a version's creation is visible to a transaction's snapshot
   *
   * True if the transaction created it, or if it was committed by another
   * transaction at a begin_ts <= the reader's start_ts. A version whose
   * creation is not yet visible (uncommitted by another txn, committed after
   * the reader started, or rolled back) is invisible; the reader should walk
   * to an older version in the chain.
   */
  [[nodiscard]] bool is_created_visible(const VersionInfo &version,
                                        const Transaction *txn) const;

  /**
   * @brief Whether a version's deletion is visible to a transaction's snapshot
   *
   * True if the transaction itself deleted it, or another transaction's delete
   * committed at an end_ts <= the reader's start_ts. When the creation is
   * visible but the deletion is too, the row is gone from the reader's view.
   */
  [[nodiscard]] bool is_delete_visible(const VersionInfo &version,
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
   * @brief Finalize a version's timestamps when a transaction commits
   *
   * Distinguishes the creator of the version from its deleter, using @p txn's
   * id: the creator's commit stamps begin_ts, the deleter's commit stamps
   * end_ts. Finalizing a commit therefore never affects a version this
   * transaction did not create or delete (fixes the creator/deleter
   * conflation in #27).
   *
   * @param version   The version info to finalize
   * @param txn       The committing transaction
   * @param commit_ts The commit timestamp (from @ref get_timestamp)
   */
  void finalize_commit(VersionInfo &version, const Transaction *txn,
                       uint64_t commit_ts);

  /**
   * @brief Roll back a version touched by an aborting transaction
   *
   * Distinguishes creator from deleter: if @p txn created the version it is
   * invalidated (never visible); if @p txn only deleted a version created by
   * a different, committed transaction, the delete is undone and the
   * committed version survives (fixes aborted-DELETE data loss in #27).
   *
   * @param version The version to roll back
   * @param txn     The aborting transaction
   */
  void rollback_version(VersionInfo &version, const Transaction *txn);

private:
  /// Global monotonic timestamp counter. Starts one past the pre-history
  /// sentinel so the first allocated timestamp is strictly greater than
  /// TIMESTAMP_PREHISTORY and can never alias a synthetic base version.
  std::atomic<uint64_t> global_timestamp_{TIMESTAMP_PREHISTORY + 1};
};

} // namespace entropy
