/**
 * @file update_executor.cpp
 * @brief Update executor implementation
 */

#include "execution/update_executor.hpp"

#include <vector>

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

    // Pre-mutation: row lock + first-updater-wins registration against the
    // before-image. A conflict aborts before the heap is touched, and the
    // lock is held until commit/abort so no other writer can interleave
    // between this check and the mutation.
    Status status =
        txn_acquire_write(ctx_, table_oid_, rid, tuple, TxnWriteKind::kUpdate);
    if (!status.ok()) {
      status_ = status;
      break;
    }

    status = table_heap_->update_tuple_in_place(new_tuple, rid);
    if (status.ok()) {
      // In-place update: one UPDATE record at the original RID.
      rows_updated_++;
      txn_log_update(ctx_, table_oid_, rid, tuple, new_tuple);
      continue;
    }
    if (status.code() != StatusCode::kOutOfMemory) {
      status_ = status; // row vanished or page error: surface it
      break;
    }

    // The new bytes do not fit in place: relocate as an explicit, logged
    // delete+insert. A single UPDATE record at the old RID could not be
    // redone (the old slot is empty after replaying the delete), and the new
    // RID needs its own version metadata or its uncommitted bytes would be
    // exposed to every snapshot. Order: convert the pending update on the
    // old chain into a delete, free the old slot (older snapshots keep
    // reading the retained before-image through the chain), log the delete
    // AFTER the slot is freed (so the stamped page LSN never claims an
    // unapplied delete), then insert+publish the new location and lock it.
    status = txn_acquire_write(ctx_, table_oid_, rid, tuple,
                               TxnWriteKind::kDelete);
    if (!status.ok()) {
      status_ = status;
      break;
    }
    status = table_heap_->delete_tuple(rid);
    if (!status.ok()) {
      status_ = status;
      break;
    }
    txn_log_delete(ctx_, table_oid_, rid, tuple);

    RID new_rid;
    status = table_heap_->insert_tuple(
        new_tuple, &new_rid, txn_insert_hook(ctx_, table_oid_, new_tuple));
    if (!status.ok()) {
      status_ = status;
      break;
    }
    status = txn_lock_row(ctx_, table_oid_, new_rid);
    if (!status.ok()) {
      status_ = status;
      break;
    }
    rows_updated_++;
  }

  done_ = true;
  return std::nullopt;
}

} // namespace entropy
