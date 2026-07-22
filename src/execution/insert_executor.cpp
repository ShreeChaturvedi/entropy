/**
 * @file insert_executor.cpp
 * @brief Insert executor implementation
 */

#include "execution/insert_executor.hpp"

#include "execution/executor_context.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction/version_store.hpp"

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

    // When run inside a transaction, lock the freshly placed row, register the
    // new version, and log the insert (WAL + write-set for undo). A null
    // context skips all of this (executor unit tests).
    if (ctx_ != nullptr && ctx_->txn != nullptr) {
      if (ctx_->lock_mgr != nullptr) {
        Status locked = ctx_->lock_mgr->lock_row(ctx_->txn, table_oid_, rid,
                                                 LockMode::EXCLUSIVE);
        if (!locked.ok()) {
          status_ = locked;
          break;
        }
      }
      if (ctx_->version_store != nullptr) {
        Status versioned = ctx_->version_store->on_insert(ctx_->txn, rid);
        if (!versioned.ok()) {
          status_ = versioned;
          break;
        }
      }
      if (ctx_->txn_mgr != nullptr) {
        std::vector<char> bytes(tuple.data(), tuple.data() + tuple.size());
        (void)ctx_->txn_mgr->log_insert(ctx_->txn, table_oid_, rid, bytes);
      }
    }

    rows_inserted_++;
    current_idx_++;
  }

  done_ = true;
  return std::nullopt;
}

} // namespace entropy
