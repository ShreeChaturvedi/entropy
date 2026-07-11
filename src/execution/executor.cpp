/**
 * @file executor.cpp
 * @brief Executor helpers shared across the execution engine
 *
 * Hosts the read-side snapshot resolution (mvcc_visible) and the write-side
 * transactional steps (txn_acquire_write / txn_insert_hook / txn_log_*)
 * so the individual executors carry no duplicated lock/version/WAL plumbing.
 */

#include "execution/executor.hpp"

#include <span>
#include <vector>

#include "execution/executor_context.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction/version_store.hpp"

namespace entropy {

namespace {

/// True when the statement runs inside a transaction (helpers no-op
/// otherwise, which is the executor-unit-test mode).
[[nodiscard]] bool transactional(const ExecutorContext *ctx) {
  return ctx != nullptr && ctx->txn != nullptr;
}

[[nodiscard]] std::vector<char> tuple_bytes(const Tuple &tuple) {
  return {tuple.data(), tuple.data() + tuple.size()};
}

/// Row-level exclusive lock, held until commit/abort. Idempotent for a lock
/// this transaction already holds.
[[nodiscard]] Status lock_row_exclusive(const ExecutorContext *ctx,
                                        oid_t table_oid, RID rid) {
  if (ctx->lock_mgr == nullptr) {
    return Status::Ok();
  }
  return ctx->lock_mgr->lock_row(ctx->txn, table_oid, rid,
                                 LockMode::EXCLUSIVE);
}

} // namespace

std::optional<Tuple> mvcc_visible(const ExecutorContext *ctx,
                                  const Tuple &heap_tuple) {
  // No transaction context: yield the latest (heap) version unchanged. This is
  // the path executor unit tests take when constructing executors with a null
  // context, and the path any read takes when MVCC is not wired. An empty
  // tuple (a probed ghost slot) is never a row outside MVCC.
  if (!transactional(ctx) || ctx->version_store == nullptr) {
    if (heap_tuple.is_empty()) {
      return std::nullopt;
    }
    return heap_tuple;
  }

  auto visible = ctx->version_store->read_visible(heap_tuple.rid(),
                                                  heap_tuple.as_span(), ctx->txn);
  if (!visible.has_value()) {
    return std::nullopt; // invisible to this snapshot
  }
  // read_visible returns owned bytes (a before-image or a copy of the heap
  // bytes), so wrap them in a tuple carrying the same RID.
  return Tuple(std::move(*visible), heap_tuple.rid());
}

Status txn_acquire_write(const ExecutorContext *ctx, oid_t table_oid, RID rid,
                         const Tuple &before, TxnWriteKind kind) {
  if (!transactional(ctx)) {
    return Status::Ok();
  }

  Status locked = lock_row_exclusive(ctx, table_oid, rid);
  if (!locked.ok()) {
    return locked;
  }

  if (ctx->version_store != nullptr) {
    const std::span<const char> image(before.data(), before.size());
    return kind == TxnWriteKind::kUpdate
               ? ctx->version_store->on_update(ctx->txn, rid, image)
               : ctx->version_store->on_delete(ctx->txn, rid, image);
  }
  return Status::Ok();
}

std::function<Status(RID)> txn_insert_hook(const ExecutorContext *ctx,
                                           oid_t table_oid,
                                           const Tuple &tuple) {
  if (!transactional(ctx)) {
    return nullptr;
  }
  // Runs inside the heap's exclusive-lock critical section: version first
  // (the instant a reader could see the bytes, the chain already marks them
  // uncommitted), then the WAL record + write-set entry. Logging also stamps
  // the page LSN while the page is still pinned, so the WAL-before-page rule
  // covers the insert from its first flushable moment. No lock waits here —
  // txn_lock_row runs after the heap lock is released.
  return [ctx, table_oid, &tuple](RID rid) -> Status {
    if (ctx->version_store != nullptr) {
      Status versioned = ctx->version_store->on_insert(ctx->txn, rid);
      if (!versioned.ok()) {
        return versioned;
      }
    }
    if (ctx->txn_mgr != nullptr) {
      (void)ctx->txn_mgr->log_insert(ctx->txn, table_oid, rid,
                                     tuple_bytes(tuple));
    }
    return Status::Ok();
  };
}

Status txn_lock_row(const ExecutorContext *ctx, oid_t table_oid, RID rid) {
  if (!transactional(ctx)) {
    return Status::Ok();
  }
  return lock_row_exclusive(ctx, table_oid, rid);
}

void txn_log_update(const ExecutorContext *ctx, oid_t table_oid, RID rid,
                    const Tuple &before, const Tuple &after) {
  if (transactional(ctx) && ctx->txn_mgr != nullptr) {
    (void)ctx->txn_mgr->log_update(ctx->txn, table_oid, rid,
                                   tuple_bytes(before), tuple_bytes(after));
  }
}

void txn_log_delete(const ExecutorContext *ctx, oid_t table_oid, RID rid,
                    const Tuple &before) {
  if (transactional(ctx) && ctx->txn_mgr != nullptr) {
    (void)ctx->txn_mgr->log_delete(ctx->txn, table_oid, rid,
                                   tuple_bytes(before));
  }
}

} // namespace entropy
