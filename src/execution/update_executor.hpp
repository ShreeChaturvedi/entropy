#pragma once

/**
 * @file update_executor.hpp
 * @brief UPDATE statement executor
 */

#include <memory>
#include <vector>

#include "execution/executor.hpp"
#include "parser/expression.hpp"
#include "storage/table_heap.hpp"

namespace entropy {

/**
 * @brief Update executor - updates tuples matching a predicate
 *
 * Uses a child executor (typically SeqScan with predicate) to find
 * tuples to update, then applies the SET expressions.
 */
class UpdateExecutor : public Executor {
public:
  /**
   * @brief Construct an UPDATE executor
   * @param ctx Executor context
   * @param child Child executor providing tuples to update
   * @param table_heap Table to update
   * @param schema Table schema
   * @param column_indices Indices of columns to update
   * @param values Expressions to evaluate for new values
   */
  UpdateExecutor(ExecutorContext *ctx, std::unique_ptr<Executor> child,
                 std::shared_ptr<TableHeap> table_heap, const Schema *schema,
                 std::vector<size_t> column_indices,
                 std::vector<std::unique_ptr<Expression>> values)
      : Executor(ctx), child_(std::move(child)),
        table_heap_(std::move(table_heap)), schema_(schema),
        column_indices_(std::move(column_indices)), values_(std::move(values)) {
  }

  /**
   * @brief Initialize the executor
   */
  void init() override;

  /**
   * @brief Execute updates
   * @return nullopt when done
   */
  [[nodiscard]] std::optional<Tuple> next() override;

  /**
   * @brief Get number of rows updated
   */
  [[nodiscard]] size_t rows_updated() const { return rows_updated_; }

private:
  std::unique_ptr<Executor> child_;
  std::shared_ptr<TableHeap> table_heap_;
  const Schema *schema_;
  std::vector<size_t> column_indices_;
  std::vector<std::unique_ptr<Expression>> values_;
  size_t rows_updated_ = 0;
  bool done_ = false;
};

} // namespace entropy
