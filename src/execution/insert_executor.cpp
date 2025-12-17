/**
 * @file insert_executor.cpp
 * @brief Insert executor implementation
 */

#include "execution/insert_executor.hpp"

namespace entropy {

void InsertExecutor::init() {
  current_idx_ = 0;
  rows_inserted_ = 0;
  done_ = false;
}

std::optional<Tuple> InsertExecutor::next() {
  if (done_) {
    return std::nullopt;
  }

  // Insert all tuples
  while (current_idx_ < tuples_.size()) {
    RID rid;
    Status status = table_heap_->insert_tuple(tuples_[current_idx_], &rid);
    if (status.ok()) {
      rows_inserted_++;
    }
    current_idx_++;
  }

  done_ = true;

  // Return a tuple with the row count
  // For simplicity, we return an empty tuple
  // A full implementation would return a count tuple
  return std::nullopt;
}

} // namespace entropy
