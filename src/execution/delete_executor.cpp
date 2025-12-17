/**
 * @file delete_executor.cpp
 * @brief Delete executor implementation
 */

#include "execution/delete_executor.hpp"

namespace entropy {

void DeleteExecutor::init() {
  rows_deleted_ = 0;
  done_ = false;
  if (child_) {
    child_->init();
  }
}

std::optional<Tuple> DeleteExecutor::next() {
  if (done_) {
    return std::nullopt;
  }

  // Delete all tuples from child
  while (auto tuple = child_->next()) {
    RID rid = tuple->rid();
    Status status = table_heap_->delete_tuple(rid);
    if (status.ok()) {
      rows_deleted_++;
    }
  }

  done_ = true;
  return std::nullopt;
}

} // namespace entropy
