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

    RID new_rid = rid;
    status = table_heap_->update_tuple(new_tuple, rid, &new_rid);
    if (!status.ok()) {
      status_ = status;
      break;
    }
    rows_updated_++;

    if (new_rid == rid) {
      // In-place update: one UPDATE record at the original RID.
      txn_log_update(ctx_, table_oid_, rid, tuple, new_tuple);
    } else {
      // Grow-update relocated the row (the heap did delete@old + insert@new).
      // Record it as such: a single UPDATE record at the old RID could not be
      // redone (the old slot is empty after replaying the delete), and the
      // new RID would otherwise carry no version metadata and expose the
      // uncommitted bytes to every snapshot. Convert the pending update on
      // the old chain into a delete, log it, then register the new location
      // as an insert (lock + version + log).
      status = txn_acquire_write(ctx_, table_oid_, rid, tuple,
                                 TxnWriteKind::kDelete);
      if (status.ok()) {
        txn_log_delete(ctx_, table_oid_, rid, tuple);
        status = txn_register_insert(ctx_, table_oid_, new_rid, new_tuple);
      }
      if (!status.ok()) {
        status_ = status;
        break;
      }
    }
  }

  done_ = true;
  return std::nullopt;
}

} // namespace entropy
