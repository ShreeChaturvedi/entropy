/**
 * @file filter.cpp
 * @brief Filter executor implementation
 */

#include "execution/filter.hpp"

namespace entropy {

void FilterExecutor::init() {
  if (child_) {
    child_->init();
  }
}

std::optional<Tuple> FilterExecutor::next() {
  while (auto tuple = child_->next()) {
    // Evaluate predicate
    TupleValue result = predicate_->evaluate(*tuple, *schema_);

    // Return tuple if predicate is true. A non-boolean or NULL predicate value
    // is treated as false (matches NestedLoopJoinExecutor) rather than throwing
    // bad_variant_access from as_bool().
    if (result.is_bool() && result.as_bool()) {
      return tuple;
    }
  }

  return std::nullopt;
}

} // namespace entropy
