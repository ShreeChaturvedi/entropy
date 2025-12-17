/**
 * @file limit_executor.cpp
 * @brief Limit Executor implementation
 */

#include "execution/limit_executor.hpp"

namespace entropy {

LimitExecutor::LimitExecutor(ExecutorContext *ctx,
                             std::unique_ptr<Executor> child,
                             std::optional<size_t> limit, size_t offset)
    : Executor(ctx), child_(std::move(child)), limit_(limit), offset_(offset) {}

void LimitExecutor::init() {
  skipped_ = 0;
  returned_ = 0;
  child_->init();
}

std::optional<Tuple> LimitExecutor::next() {
  // Check if we've already returned enough
  if (limit_.has_value() && returned_ >= *limit_) {
    return std::nullopt;
  }

  // Skip OFFSET rows
  while (skipped_ < offset_) {
    auto tuple = child_->next();
    if (!tuple.has_value()) {
      return std::nullopt; // Exhausted before reaching offset
    }
    skipped_++;
  }

  // Return next row (if within limit)
  auto tuple = child_->next();
  if (tuple.has_value()) {
    returned_++;
  }
  return tuple;
}

} // namespace entropy
