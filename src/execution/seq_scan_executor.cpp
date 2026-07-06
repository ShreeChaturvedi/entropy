/**
 * @file seq_scan_executor.cpp
 * @brief Sequential scan executor implementation
 */

#include "execution/seq_scan_executor.hpp"

namespace entropy {

void SeqScanExecutor::init() {
  if (table_heap_) {
    iterator_ = table_heap_->begin();
    end_ = table_heap_->end();
  }
}

std::optional<Tuple> SeqScanExecutor::next() {
  while (iterator_ != end_) {
    Tuple tuple = *iterator_;
    ++iterator_;

    // If no predicate, return all tuples
    if (!predicate_) {
      return tuple;
    }

    // Keep the tuple only when the predicate evaluates to boolean true.
    TupleValue result = predicate_->evaluate(tuple, *schema_);
    if (predicate_is_true(result)) {
      return tuple;
    }
  }

  return std::nullopt;
}

} // namespace entropy
