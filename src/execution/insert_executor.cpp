/**
 * @file insert_executor.cpp
 * @brief Insert executor implementation
 */

#include "execution/insert_executor.hpp"

namespace entropy {

void InsertExecutor::init() {
  current_idx_ = 0;
  rows_inserted_ = 0;
  done_ = false;
  status_ = Status::Ok();
}

std::optional<Tuple> InsertExecutor::next() {
  if (done_) {
    return std::nullopt;
  }

  // Insert all tuples
  while (current_idx_ < tuples_.size()) {
    const Tuple &tuple = tuples_[current_idx_];
    RID rid;
    // The publication hook registers the uncommitted version and logs the
    // insert inside the heap's critical section, so no concurrent reader can
    // observe the bytes before their version metadata exists. A no-op hook
    // without a transaction context (executor unit tests).
    Status status = table_heap_->insert_tuple(
        tuple, &rid, txn_insert_hook(ctx_, table_oid_, tuple));
    if (!status.ok()) {
      status_ = status;
      break;
    }

    // Row lock last: lock waits must not happen inside the heap lock. The
    // row is already invisible to other snapshots, and on failure the abort
    // undoes the published insert via the write set.
    status = txn_lock_row(ctx_, table_oid_, rid);
    if (!status.ok()) {
      status_ = status;
      break;
    }

    rows_inserted_++;
    current_idx_++;
  }

  done_ = true;
  return std::nullopt;
}

} // namespace entropy
