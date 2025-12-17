#pragma once

/**
 * @file limit_executor.hpp
 * @brief Limit Executor for LIMIT/OFFSET
 *
 * Efficient streaming implementation:
 * - No materialization required
 * - Skips OFFSET rows, returns up to LIMIT rows
 * - O(1) per tuple overhead
 */

#include "execution/executor.hpp"

namespace entropy {

/**
 * @brief Limit Executor
 *
 * Applies LIMIT and OFFSET constraints to output.
 * Streams results without materializing.
 */
class LimitExecutor : public Executor {
public:
  /**
   * @brief Construct limit executor
   * @param ctx Execution context
   * @param child Child executor
   * @param limit Maximum rows to return (nullopt = unlimited)
   * @param offset Rows to skip before returning
   */
  LimitExecutor(ExecutorContext *ctx, std::unique_ptr<Executor> child,
                std::optional<size_t> limit, size_t offset = 0);

  void init() override;
  std::optional<Tuple> next() override;

private:
  std::unique_ptr<Executor> child_;
  std::optional<size_t> limit_;
  size_t offset_;

  size_t skipped_ = 0;
  size_t returned_ = 0;
};

} // namespace entropy
