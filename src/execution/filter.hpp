#pragma once

/**
 * @file filter.hpp
 * @brief Filter executor - applies a predicate to filter tuples
 */

#include <memory>

#include "execution/executor.hpp"
#include "parser/expression.hpp"

namespace entropy {

/**
 * @brief Filter executor - filters tuples based on a predicate
 *
 * Wraps a child executor and only passes through tuples that
 * satisfy the given predicate expression.
 */
class FilterExecutor : public Executor {
public:
  /**
   * @brief Construct a filter executor
   * @param ctx Executor context
   * @param child Child executor providing tuples
   * @param predicate Filter expression (must evaluate to boolean)
   * @param schema Schema for evaluation
   */
  FilterExecutor(ExecutorContext *ctx, std::unique_ptr<Executor> child,
                 std::unique_ptr<Expression> predicate, const Schema *schema)
      : Executor(ctx), child_(std::move(child)),
        predicate_(std::move(predicate)), schema_(schema) {}

  /**
   * @brief Initialize the executor
   */
  void init() override;

  /**
   * @brief Get the next tuple that satisfies the predicate
   * @return Next matching tuple, or nullopt if no more
   */
  [[nodiscard]] std::optional<Tuple> next() override;

private:
  std::unique_ptr<Executor> child_;
  std::unique_ptr<Expression> predicate_;
  const Schema *schema_;
};

} // namespace entropy
