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
    Status status = table_heap_->insert_tuple(tuple, &rid);
    if (!status.ok()) {
      status_ = status;
      break;
    }

    // Inside a transaction: lock the freshly placed row, register the new
    // (uncommitted) version, and log the insert for WAL redo and abort undo.
    // A no-op without a transaction context (executor unit tests).
    status = txn_register_insert(ctx_, table_oid_, rid, tuple);
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
