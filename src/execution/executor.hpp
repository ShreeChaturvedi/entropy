#pragma once

/**
 * @file executor.hpp
 * @brief Executor interface
 */

#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>

#include "entropy/status.hpp"
#include "storage/tuple.hpp"

namespace entropy {

class ExecutorContext;

/**
 * @brief True only when a predicate evaluated to boolean true.
 *
 * A NULL or non-boolean predicate value is treated as false (SQL three-valued
 * logic), so callers never invoke as_bool() on a non-bool variant and trigger
 * std::bad_variant_access. Shared by the executors that apply a WHERE/JOIN
 * predicate (filter, seq scan, nested loop join) so the guard stays consistent.
 */
inline bool predicate_is_true(const TupleValue& value) {
    return value.is_bool() && value.as_bool();
}

/**
 * @brief Resolve the version of @p heap_tuple visible to @p ctx's snapshot
 *
 * When @p ctx has a transaction and a version store, consults the store's
 * per-RID chain and returns the tuple the snapshot is entitled to see (the heap
 * bytes for the live head, or a retained before-image for an older snapshot),
 * or nullopt when the row is invisible (created after the snapshot, or deleted
 * from its view). With no context, no transaction, or no version store, returns
 * @p heap_tuple unchanged — the behaviour executor unit tests rely on when they
 * drive an executor directly against a table heap.
 */
[[nodiscard]] std::optional<Tuple> mvcc_visible(const ExecutorContext *ctx,
                                                const Tuple &heap_tuple);

// ─────────────────────────────────────────────────────────────────────────────
// Transactional write helpers (the write-side counterparts of mvcc_visible)
// ─────────────────────────────────────────────────────────────────────────────
//
// Each helper is a no-op returning Ok() when @p ctx (or its transaction) is
// null, and internally guards every context member, so executors carry no
// per-callsite null pyramids and unit tests can keep driving them without a
// context.

/// Which mutation a writer is about to apply to an existing row.
enum class TxnWriteKind { kUpdate, kDelete };

/**
 * @brief Pre-mutation step for UPDATE/DELETE on an existing row
 *
 * Acquires the row-level exclusive lock, then registers the write intent with
 * the version store (retaining @p before as the before-image) under
 * first-updater-wins: a conflicting concurrent change yields Status::Aborted
 * and the heap must not be touched. Must run BEFORE the heap mutation; the
 * lock is held until commit/abort, so no other writer can interleave between
 * this check and the mutation.
 */
[[nodiscard]] Status txn_acquire_write(const ExecutorContext *ctx,
                                       oid_t table_oid, RID rid,
                                       const Tuple &before, TxnWriteKind kind);

/**
 * @brief Publication hook for a transactional INSERT
 *
 * Returns the hook to pass to TableHeap::insert_tuple. It runs while the heap
 * still holds its exclusive lock — before any reader can observe the new
 * slot — and registers the uncommitted version (so the row is invisible to
 * every other snapshot from its first reachable instant) and logs the insert
 * to the WAL and the write set (undo = delete). Returns an empty function
 * when there is no transaction. @p tuple must outlive the insert_tuple call
 * the hook is passed to.
 *
 * Row locking is NOT part of the hook (a lock wait must never happen inside
 * the heap's critical section); call txn_lock_row after the insert returns.
 */
[[nodiscard]] std::function<Status(RID)>
txn_insert_hook(const ExecutorContext *ctx, oid_t table_oid,
                const Tuple &tuple);

/**
 * @brief Acquire the row-level exclusive lock (held until commit/abort)
 *
 * Idempotent for a lock the transaction already holds; no-op without a
 * transaction context or lock manager.
 */
[[nodiscard]] Status txn_lock_row(const ExecutorContext *ctx, oid_t table_oid,
                                  RID rid);

/**
 * @brief The checkpoint writer-quiesce barrier for @p ctx, or nullptr
 *
 * Passed into the heap write methods so the page mutation + WAL append + page-
 * LSN stamp run under one shared-barrier hold, mutually exclusive with a
 * checkpoint's page flush (crash-safety F3). Null (no barrier) without a
 * transaction context or transaction manager — the executor-unit-test path.
 */
[[nodiscard]] std::shared_mutex *
txn_checkpoint_barrier(const ExecutorContext *ctx);

/**
 * @brief Predicate that keeps an INSERT's free-slot search off slots reserved
 *        by an uncommitted DELETE (crash-safety F1), or nullptr
 *
 * Null without a transaction context or version store, so executor unit tests
 * reuse freed slots exactly as before.
 */
[[nodiscard]] std::function<bool(RID)>
txn_slot_reserved(const ExecutorContext *ctx);

/// Log an in-place UPDATE to the WAL and the write set (undo = restore
/// @p before). Called after the heap mutation succeeded.
void txn_log_update(const ExecutorContext *ctx, oid_t table_oid, RID rid,
                    const Tuple &before, const Tuple &after);

/// Log a DELETE to the WAL and the write set (undo = re-insert @p before).
/// Called after the heap mutation succeeded.
void txn_log_delete(const ExecutorContext *ctx, oid_t table_oid, RID rid,
                    const Tuple &before);

/**
 * @brief Base class for all executors
 */
class Executor {
public:
    explicit Executor(ExecutorContext* ctx) : ctx_(ctx) {}
    virtual ~Executor() = default;

    virtual void init() = 0;
    virtual std::optional<Tuple> next() = 0;

protected:
    ExecutorContext* ctx_;
};

}  // namespace entropy
