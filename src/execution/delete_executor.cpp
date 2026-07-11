/**
 * @file delete_executor.cpp
 * @brief Delete executor implementation
 */

#include "execution/delete_executor.hpp"

#include <vector>

namespace entropy {

void DeleteExecutor::init() {
  rows_deleted_ = 0;
  done_ = false;
  status_ = Status::Ok();
  if (child_) {
    child_->init();
  }
}

std::optional<Tuple> DeleteExecutor::next() {
  if (done_) {
    return std::nullopt;
  }

  // Materialize matching rows first so the delete cannot perturb the child scan
  // mid-iteration (mirrors UpdateExecutor's Halloween guard).
  std::vector<Tuple> to_delete;
  while (auto tuple = child_->next()) {
    to_delete.push_back(std::move(*tuple));
  }

  // Loop-invariant: the barrier keeps each free + WAL log atomic against a
  // checkpoint (F3). Null without a transaction context (executor unit tests).
  std::shared_mutex *barrier = txn_checkpoint_barrier(ctx_);

  for (const auto &tuple : to_delete) {
    RID rid = tuple.rid();

    // Pre-mutation: row lock + first-updater-wins registration. A conflict
    // aborts before the heap is touched.
    Status status =
        txn_acquire_write(ctx_, table_oid_, rid, tuple, TxnWriteKind::kDelete);
    if (!status.ok()) {
      status_ = status;
      break;
    }

    // The free + WAL log run under one checkpoint-barrier hold so the freed
    // slot cannot be checkpoint-flushed ahead of its DELETE record (F3).
    status = table_heap_->delete_tuple(
        rid, [&] { txn_log_delete(ctx_, table_oid_, rid, tuple); }, barrier);
    if (!status.ok()) {
      status_ = status;
      break;
    }
    rows_deleted_++;
  }

  done_ = true;
  return std::nullopt;
}

} // namespace entropy
