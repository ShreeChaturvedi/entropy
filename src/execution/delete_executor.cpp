/**
 * @file delete_executor.cpp
 * @brief Delete executor implementation
 */

#include "execution/delete_executor.hpp"

#include <span>
#include <vector>

#include "execution/executor_context.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction/version_store.hpp"

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

  const bool transactional = ctx_ != nullptr && ctx_->txn != nullptr;

  // Materialize matching rows first so the delete cannot perturb the child scan
  // mid-iteration (mirrors UpdateExecutor's Halloween guard).
  std::vector<Tuple> to_delete;
  while (auto tuple = child_->next()) {
    to_delete.push_back(std::move(*tuple));
  }

  for (const auto &tuple : to_delete) {
    RID rid = tuple.rid();

    // Transactional path: lock, then first-updater-wins conflict check on the
    // row being deleted, before mutating the heap.
    if (transactional) {
      if (ctx_->lock_mgr != nullptr) {
        Status locked = ctx_->lock_mgr->lock_row(ctx_->txn, table_oid_, rid,
                                                 LockMode::EXCLUSIVE);
        if (!locked.ok()) {
          status_ = locked;
          break;
        }
      }
      if (ctx_->version_store != nullptr) {
        std::span<const char> before(tuple.data(), tuple.size());
        Status versioned = ctx_->version_store->on_delete(ctx_->txn, rid, before);
        if (!versioned.ok()) {
          status_ = versioned; // write-write conflict
          break;
        }
      }
    }

    Status status = table_heap_->delete_tuple(rid);
    if (!status.ok()) {
      status_ = status;
      break;
    }
    rows_deleted_++;

    if (transactional && ctx_->txn_mgr != nullptr) {
      std::vector<char> old_bytes(tuple.data(), tuple.data() + tuple.size());
      (void)ctx_->txn_mgr->log_delete(ctx_->txn, table_oid_, rid, old_bytes);
    }
  }

  done_ = true;
  return std::nullopt;
}

} // namespace entropy
