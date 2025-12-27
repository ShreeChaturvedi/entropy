/**
 * @file sort_executor.cpp
 * @brief Sort Executor implementation
 *
 * Uses std::sort with a custom comparator for multi-column ordering.
 * Handles NULL values: NULLs sort first in ASC, last in DESC (SQL standard).
 */

#include "execution/sort_executor.hpp"

#include <algorithm>

namespace entropy {

SortExecutor::SortExecutor(ExecutorContext *ctx,
                           std::unique_ptr<Executor> child,
                           const Schema *schema, std::vector<SortKey> sort_keys)
    : Executor(ctx), child_(std::move(child)), schema_(schema),
      sort_keys_(std::move(sort_keys)) {}

void SortExecutor::init() {
  sorted_tuples_.clear();
  current_index_ = 0;
  initialized_ = true;
  child_->init();

  // Materialize all input tuples
  while (auto tuple = child_->next()) {
    sorted_tuples_.push_back(std::move(*tuple));
  }

  // Sort using custom comparator
  if (!sort_keys_.empty()) {
    std::sort(sorted_tuples_.begin(), sorted_tuples_.end(),
              [this](const Tuple &a, const Tuple &b) { return compare(a, b); });
  }
}

std::optional<Tuple> SortExecutor::next() {
  if (!initialized_ || current_index_ >= sorted_tuples_.size()) {
    return std::nullopt;
  }
  return std::move(sorted_tuples_[current_index_++]);
}

bool SortExecutor::compare(const Tuple &a, const Tuple &b) const {
  for (const auto &key : sort_keys_) {
    TupleValue val_a =
        a.get_value(*schema_, static_cast<uint32_t>(key.column_index));
    TupleValue val_b =
        b.get_value(*schema_, static_cast<uint32_t>(key.column_index));

    // Handle NULLs: NULLs sort first in ASC, last in DESC
    if (val_a.is_null() && val_b.is_null())
      continue;
    if (val_a.is_null())
      return key.ascending; // NULL < any in ASC
    if (val_b.is_null())
      return !key.ascending; // any < NULL in ASC is false

    // Compare non-NULL values
    bool less = false;
    bool equal = false;

    // Numeric comparison
    if (val_a.is_integer() || val_a.is_bigint() || val_a.is_smallint() ||
        val_a.is_tinyint()) {
      int64_t a_num = val_a.is_integer()    ? val_a.as_integer()
                      : val_a.is_bigint()   ? val_a.as_bigint()
                      : val_a.is_smallint() ? val_a.as_smallint()
                                            : val_a.as_tinyint();
      int64_t b_num = val_b.is_integer()    ? val_b.as_integer()
                      : val_b.is_bigint()   ? val_b.as_bigint()
                      : val_b.is_smallint() ? val_b.as_smallint()
                                            : val_b.as_tinyint();
      less = a_num < b_num;
      equal = a_num == b_num;
    } else if (val_a.is_float() || val_a.is_double()) {
      double a_num = val_a.is_double() ? val_a.as_double()
                                       : static_cast<double>(val_a.as_float());
      double b_num = val_b.is_double() ? val_b.as_double()
                                       : static_cast<double>(val_b.as_float());
      less = a_num < b_num;
      equal = a_num == b_num;
    } else if (val_a.is_string()) {
      less = val_a.as_string() < val_b.as_string();
      equal = val_a.as_string() == val_b.as_string();
    } else if (val_a.is_bool()) {
      // false < true
      less = !val_a.as_bool() && val_b.as_bool();
      equal = val_a.as_bool() == val_b.as_bool();
    }

    if (!equal) {
      return key.ascending ? less : !less;
    }
    // Equal on this key, continue to next
  }

  return false; // All keys equal
}

} // namespace entropy
