/**
 * @file update_executor.cpp
 * @brief Update executor implementation
 */

#include "execution/update_executor.hpp"

#include <span>
#include <vector>

#include "execution/executor_context.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction/version_store.hpp"

namespace entropy {

namespace {
// Helper to convert a TupleValue to a specific type
TupleValue convert_to_type(const TupleValue &val, TypeId target_type) {
  if (val.is_null())
    return val;

  // Handle integer conversions
  if (val.is_bigint()) {
    int64_t v = val.as_bigint();
    switch (target_type) {
    case TypeId::TINYINT:
      return TupleValue(static_cast<int8_t>(v));
    case TypeId::SMALLINT:
      return TupleValue(static_cast<int16_t>(v));
    case TypeId::INTEGER:
      return TupleValue(static_cast<int32_t>(v));
    default:
      return val;
    }
  }

  // Handle double to float conversion
  if (val.is_double() && target_type == TypeId::FLOAT) {
    return TupleValue(static_cast<float>(val.as_double()));
  }

  return val;
}
} // namespace

void UpdateExecutor::init() {
  rows_updated_ = 0;
  done_ = false;
  status_ = Status::Ok();
  if (child_) {
    child_->init();
  }
}

std::optional<Tuple> UpdateExecutor::next() {
  if (done_) {
    return std::nullopt;
  }

  // Phase 1: materialize matching tuples (and their RIDs) before mutating the
  // heap. Grow-updates delete+reinsert onto a later page; if we updated while
  // the child SeqScan was still walking the same heap, those relocated rows
  // would be re-encountered and updated again (Halloween problem).
  std::vector<Tuple> to_update;
  while (auto tuple = child_->next()) {
    to_update.push_back(std::move(*tuple));
  }

  const bool transactional = ctx_ != nullptr && ctx_->txn != nullptr;

  // Phase 2: apply SET expressions to the snapshotted rows only.
  for (const auto &tuple : to_update) {
    RID rid = tuple.rid();

    // Build new tuple values
    std::vector<TupleValue> new_values;
    new_values.reserve(schema_->column_count());

    // Start with original values
    for (size_t i = 0; i < schema_->column_count(); i++) {
      new_values.push_back(
          tuple.get_value(*schema_, static_cast<uint32_t>(i)));
    }

    // Apply updates from SET clause with type conversion
    for (size_t i = 0; i < column_indices_.size(); i++) {
      size_t col_idx = column_indices_[i];
      TupleValue new_val = values_[i]->evaluate(tuple, *schema_);
      TypeId target_type = schema_->column(col_idx).type();
      new_values[col_idx] = convert_to_type(new_val, target_type);
    }

    Tuple new_tuple(new_values, *schema_);

    // Transactional path: lock the row, run first-updater-wins conflict
    // detection against the before-image, then mutate + log. The lock and the
    // conflict check run BEFORE the heap mutation so a conflict aborts without
    // touching the heap. The row lock is held until commit/abort, so no other
    // writer can interleave between the check and the mutation.
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
        Status versioned = ctx_->version_store->on_update(ctx_->txn, rid, before);
        if (!versioned.ok()) {
          status_ = versioned; // write-write conflict
          break;
        }
      }
    }

    RID new_rid = rid;
    Status status = table_heap_->update_tuple(new_tuple, rid, &new_rid);
    if (!status.ok()) {
      status_ = status;
      break;
    }
    rows_updated_++;

    if (transactional) {
      std::vector<char> old_bytes(tuple.data(), tuple.data() + tuple.size());
      std::vector<char> new_bytes(new_tuple.data(),
                                  new_tuple.data() + new_tuple.size());
      if (new_rid == rid) {
        // In-place update: one UPDATE record at the original RID.
        if (ctx_->txn_mgr != nullptr) {
          (void)ctx_->txn_mgr->log_update(ctx_->txn, table_oid_, rid, old_bytes,
                                          new_bytes);
        }
      } else {
        // Grow-update relocated the row (heap did delete@old + insert@new).
        // Record it as such: a single UPDATE record at the old RID could not
        // be redone (the old slot is empty after replaying the delete) and
        // the new RID would carry no version metadata, exposing the
        // uncommitted bytes to every snapshot. Lock the new RID, convert the
        // old chain's pending update into a delete, open a chain at the new
        // location, and log delete+insert so redo/undo reproduce the move.
        if (ctx_->lock_mgr != nullptr) {
          Status locked = ctx_->lock_mgr->lock_row(ctx_->txn, table_oid_,
                                                   new_rid, LockMode::EXCLUSIVE);
          if (!locked.ok()) {
            status_ = locked;
            break;
          }
        }
        if (ctx_->version_store != nullptr) {
          std::span<const char> before(old_bytes.data(), old_bytes.size());
          Status versioned =
              ctx_->version_store->on_delete(ctx_->txn, rid, before);
          if (versioned.ok()) {
            versioned = ctx_->version_store->on_insert(ctx_->txn, new_rid);
          }
          if (!versioned.ok()) {
            status_ = versioned;
            break;
          }
        }
        if (ctx_->txn_mgr != nullptr) {
          (void)ctx_->txn_mgr->log_delete(ctx_->txn, table_oid_, rid,
                                          old_bytes);
          (void)ctx_->txn_mgr->log_insert(ctx_->txn, table_oid_, new_rid,
                                          new_bytes);
        }
      }
    }
  }

  done_ = true;
  return std::nullopt;
}

} // namespace entropy
