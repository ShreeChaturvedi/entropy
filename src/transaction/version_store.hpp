#pragma once

/**
 * @file version_store.hpp
 * @brief In-memory MVCC version chains keyed by RID
 *
 * The VersionStore owns the per-tuple version history that makes Snapshot
 * Isolation work. The on-disk tuple format is untouched: the heap always holds
 * the newest version's bytes, while older versions are retained here as
 * before-images so that a transaction reading from an older snapshot can still
 * observe the value it is entitled to.
 *
 * Version chains never need to be durable. MVCC visibility only matters between
 * concurrently-live transactions; after a crash, WAL recovery reconstructs the
 * committed single-version state, at which point no snapshot can need a
 * pre-crash chain. The store is therefore reconstructed empty on restart.
 *
 * Concurrency:
 * - Reads take a shared lock and walk a chain.
 * - Mutations (on_insert/on_update/on_delete/finalize/rollback/gc) take a
 *   unique lock.
 * - Write-write conflicts are detected first-updater-wins: a writer that finds
 *   the current version created/deleted by another live transaction, or
 *   committed after the writer's snapshot, is rejected with a conflict Status.
 */

#include <optional>
#include <shared_mutex>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.hpp"
#include "common/types.hpp"
#include "transaction/mvcc.hpp"

namespace entropy {

class Transaction;

/**
 * @brief Owns per-RID MVCC version chains, mutation, and garbage collection.
 *
 * A chain is a vector of VersionNode ordered oldest-first; the back() element
 * is the current version, whose bytes live in the heap. Older nodes carry the
 * before-image bytes of the version they represent.
 */
class VersionStore {
public:
  /**
   * @brief A single version in a RID's chain.
   *
   * @c before_image holds this version's tuple bytes when they are NOT in the
   * heap (i.e. superseded versions, and deleted current versions whose heap
   * slot has been reclaimed). For a live current version the bytes live in the
   * heap and @c before_image is empty.
   */
  struct VersionNode {
    VersionInfo info;
    std::vector<char> before_image;
  };

  /**
   * @param mvcc Visibility/timestamp policy object (not owned). Its
   *             get_timestamp() is the clock that produces the commit
   *             timestamps passed to finalize().
   */
  explicit VersionStore(MVCCManager &mvcc) : mvcc_(mvcc) {}

  // Non-copyable, non-movable (holds a mutex).
  VersionStore(const VersionStore &) = delete;
  VersionStore &operator=(const VersionStore &) = delete;

  // ───────────────────────────────────────────────────────────────────────
  // Write path
  // ───────────────────────────────────────────────────────────────────────

  /**
   * @brief Register a freshly inserted tuple as a new version head.
   *
   * The new version is uncommitted (begin_ts == 0) and created by @p txn. If a
   * chain already exists for @p rid (e.g. a slot reused after a delete), the
   * new version is appended as the head.
   */
  Status on_insert(const Transaction *txn, RID rid);

  /**
   * @brief Record an in-place update, preserving the prior value.
   *
   * First-updater-wins: returns Status::Aborted on a write-write conflict.
   * Otherwise the superseded version's bytes (@p before_image) are retained on
   * the chain and a new uncommitted version head, created by @p txn, is pushed.
   * A transaction updating a version it already owns simply keeps its single
   * uncommitted head (the newest bytes live in the heap).
   */
  Status on_update(const Transaction *txn, RID rid,
                   std::span<const char> before_image);

  /**
   * @brief Mark the current version deleted by @p txn.
   *
   * First-updater-wins: returns Status::Aborted on a write-write conflict. The
   * deleted version's bytes (@p before_image) are retained so that snapshots
   * that do not yet see the delete can still read the row after the heap slot
   * is reclaimed.
   */
  Status on_delete(const Transaction *txn, RID rid,
                   std::span<const char> before_image);

  // ───────────────────────────────────────────────────────────────────────
  // Read path
  // ───────────────────────────────────────────────────────────────────────

  /**
   * @brief Return the bytes of the version visible to @p txn's snapshot.
   *
   * Walks the chain newest-first. If the current version's creation is not
   * visible, an older before-image is considered; if the visible version has
   * been deleted from @p txn's view, the row is absent (nullopt).
   *
   * @param heap_bytes Current heap bytes, returned when the visible version is
   *                   the live (non-deleted) head.
   * @return An owned copy of the visible bytes, or nullopt if no version is
   *         visible.
   *
   * @note Returns an owned copy rather than a span into store memory: the
   *       shared lock is dropped on return, so a span would be a use-after-free
   *       against a concurrent gc()/rollback() that frees the underlying
   *       before-image. The copy is the reader's to keep.
   */
  [[nodiscard]] std::optional<std::vector<char>>
  read_visible(RID rid, std::span<const char> heap_bytes,
               const Transaction *txn) const;

  // ───────────────────────────────────────────────────────────────────────
  // Commit / abort / GC
  // ───────────────────────────────────────────────────────────────────────

  /**
   * @brief Stamp commit timestamps on all versions this transaction touched.
   */
  void finalize(const Transaction *txn, uint64_t commit_ts);

  /**
   * @brief Undo all uncommitted version changes made by this transaction.
   *
   * Uncommitted creations are removed from their chains; uncommitted deletes
   * are reverted, leaving the underlying committed version intact.
   */
  void rollback(const Transaction *txn);

  /**
   * @brief Prune versions no active snapshot can observe.
   *
   * @param min_active_start_ts The smallest start_ts among active
   *        transactions (or the current clock value if none are active). Older
   *        before-images superseded by a version committed at or before this
   *        bound are removed, and chains whose row is committed-deleted at or
   *        before it are dropped entirely.
   */
  void gc(uint64_t min_active_start_ts);

  // ───────────────────────────────────────────────────────────────────────
  // Introspection (testing / diagnostics)
  // ───────────────────────────────────────────────────────────────────────

  /// Number of RIDs currently tracked.
  [[nodiscard]] size_t chain_count() const;

  /// Length of the chain for @p rid (0 if absent).
  [[nodiscard]] size_t chain_length(RID rid) const;

private:
  /// True if @p cur cannot be written by @p txn under first-updater-wins.
  [[nodiscard]] bool has_write_conflict(const VersionInfo &cur,
                                        const Transaction *txn) const;

  /// True if a version's creation is committed (a real, non-sentinel begin_ts).
  [[nodiscard]] static bool is_committed_create(const VersionInfo &v) noexcept {
    return v.begin_ts != 0 && v.begin_ts != TIMESTAMP_MAX;
  }

  /// True if a version's deletion is committed (a real, non-sentinel end_ts).
  [[nodiscard]] static bool is_committed_delete(const VersionInfo &v) noexcept {
    return v.is_deleted() && v.end_ts != TIMESTAMP_MAX && v.end_ts != 0;
  }

  MVCCManager &mvcc_;

  mutable std::shared_mutex latch_;
  std::unordered_map<RID, std::vector<VersionNode>> chains_;
  std::unordered_map<txn_id_t, std::unordered_set<RID>> txn_touched_;
};

} // namespace entropy
