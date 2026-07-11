/**
 * @file seq_scan_executor.cpp
 * @brief Sequential scan executor implementation
 */

#include "execution/seq_scan_executor.hpp"

#include "execution/executor_context.hpp"

namespace entropy {

void SeqScanExecutor::init() {
  if (table_heap_) {
    // Transactional scans also visit empty slots ("ghost mode"): a slot freed
    // by an in-flight or later-committed DELETE may still carry a version
    // chain whose retained before-image this snapshot is entitled to see.
    const bool ghosts = ctx_ != nullptr && ctx_->txn != nullptr;
    iterator_ = table_heap_->begin(ghosts);
    end_ = table_heap_->end();
  }
}

std::optional<Tuple> SeqScanExecutor::next() {
  while (iterator_ != end_) {
    Tuple heap_tuple = *iterator_;
    ++iterator_;

    // Resolve the version visible to this statement's snapshot. Without a
    // transaction context this returns the heap tuple unchanged.
    std::optional<Tuple> visible = mvcc_visible(ctx_, heap_tuple);
    if (!visible.has_value()) {
      continue; // row not visible to this snapshot
    }

    // Apply the predicate to the visible version's bytes.
    if (!predicate_) {
      return visible;
    }
    TupleValue result = predicate_->evaluate(*visible, *schema_);
    if (predicate_is_true(result)) {
      return visible;
    }
  }

  return std::nullopt;
}

} // namespace entropy
