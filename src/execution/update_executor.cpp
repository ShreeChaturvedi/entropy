/**
 * @file update_executor.cpp
 * @brief Update executor implementation
 */

#include "execution/update_executor.hpp"

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
  if (child_) {
    child_->init();
  }
}

std::optional<Tuple> UpdateExecutor::next() {
  if (done_) {
    return std::nullopt;
  }

  // Update all tuples from child
  while (auto tuple = child_->next()) {
    RID rid = tuple->rid();

    // Build new tuple values
    std::vector<TupleValue> new_values;
    new_values.reserve(schema_->column_count());

    // Start with original values
    for (size_t i = 0; i < schema_->column_count(); i++) {
      new_values.push_back(
          tuple->get_value(*schema_, static_cast<uint32_t>(i)));
    }

    // Apply updates from SET clause with type conversion
    for (size_t i = 0; i < column_indices_.size(); i++) {
      size_t col_idx = column_indices_[i];
      TupleValue new_val = values_[i]->evaluate(*tuple, *schema_);
      TypeId target_type = schema_->column(col_idx).type();
      new_values[col_idx] = convert_to_type(new_val, target_type);
    }

    // Create new tuple and update
    Tuple new_tuple(new_values, *schema_);
    Status status = table_heap_->update_tuple(new_tuple, rid);
    if (status.ok()) {
      rows_updated_++;
    }
  }

  done_ = true;
  return std::nullopt;
}

} // namespace entropy
