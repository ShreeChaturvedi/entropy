#pragma once

/**
 * @file sort_executor.hpp
 * @brief Sort Executor for ORDER BY
 *
 * Design Goals:
 * 1. Efficient in-memory sort using std::sort (introsort)
 * 2. Multi-column sort with ASC/DESC per column
 * 3. Stable sort option for deterministic results
 * 4. Materialize-then-emit pattern for volcano model
 *
 * Performance:
 * - O(n log n) time complexity
 * - O(n) space (must materialize all tuples)
 * - Uses std::sort with custom comparator (no allocation overhead)
 */

#include <vector>

#include "execution/executor.hpp"

namespace entropy {

/**
 * @brief Sort key specification
 */
struct SortKey {
  size_t column_index;
  bool ascending = true;

  SortKey(size_t idx, bool asc = true) : column_index(idx), ascending(asc) {}
};

/**
 * @brief Sort Executor
 *
 * Materializes all input tuples, sorts them, then emits in order.
 * Supports multiple sort keys with mixed ASC/DESC.
 */
class SortExecutor : public Executor {
public:
  /**
   * @brief Construct sort executor
   * @param ctx Execution context
   * @param child Child executor providing input
   * @param schema Input/output schema
   * @param sort_keys Sort key specifications (column index + direction)
   */
  SortExecutor(ExecutorContext *ctx, std::unique_ptr<Executor> child,
               const Schema *schema, std::vector<SortKey> sort_keys);

  void init() override;
  std::optional<Tuple> next() override;

  [[nodiscard]] const Schema *output_schema() const noexcept { return schema_; }

private:
  /**
   * @brief Compare two tuples according to sort keys
   * @return true if a should come before b
   */
  bool compare(const Tuple &a, const Tuple &b) const;

  std::unique_ptr<Executor> child_;
  const Schema *schema_;
  std::vector<SortKey> sort_keys_;

  std::vector<Tuple> sorted_tuples_;
  size_t current_index_ = 0;
  bool initialized_ = false;
};

} // namespace entropy
