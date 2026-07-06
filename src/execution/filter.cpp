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
    // Return the tuple only when the predicate evaluates to boolean true.
    TupleValue result = predicate_->evaluate(*tuple, *schema_);
    if (predicate_is_true(result)) {
      return tuple;
    }
  }

  return std::nullopt;
}

} // namespace entropy
