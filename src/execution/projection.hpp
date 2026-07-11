#pragma once

/**
 * @file projection.hpp
 * @brief Projection executor - selects columns or evaluates expressions
 */

#include <memory>
#include <vector>

#include "catalog/schema.hpp"
#include "execution/executor.hpp"
#include "parser/expression.hpp"

namespace entropy {

/**
 * @brief Projection executor
 *
 * Two modes:
 * - Column mode: forwards a subset of input columns (by index). Output columns
 *   inherit the input column metadata.
 * - Expression mode: evaluates one expression per output column against each
 *   input tuple. Used for computed/aliased SELECT items (e.g. `a + b AS c`).
 */
class ProjectionExecutor : public Executor {
public:
  /**
   * @brief Column-projection constructor (subset of input columns by index).
   */
  ProjectionExecutor(ExecutorContext *ctx, std::unique_ptr<Executor> child,
                     const Schema *input_schema,
                     std::vector<size_t> column_indices)
      : Executor(ctx), child_(std::move(child)), input_schema_(input_schema),
        column_indices_(std::move(column_indices)) {
    build_output_schema();
  }

  /**
   * @brief Expression-projection constructor.
   * @param expressions One expression per output column (bound to input_schema).
   * @param output_columns Output column metadata (name + type) per expression.
   */
  ProjectionExecutor(ExecutorContext *ctx, std::unique_ptr<Executor> child,
                     const Schema *input_schema,
                     std::vector<std::unique_ptr<Expression>> expressions,
                     std::vector<Column> output_columns)
      : Executor(ctx), child_(std::move(child)), input_schema_(input_schema),
        expressions_(std::move(expressions)),
        output_schema_(std::move(output_columns)) {}

  void init() override;
  [[nodiscard]] std::optional<Tuple> next() override;

  [[nodiscard]] const Schema &output_schema() const { return output_schema_; }

private:
  void build_output_schema();

  std::unique_ptr<Executor> child_;
  const Schema *input_schema_;
  std::vector<size_t> column_indices_;                  // column mode
  std::vector<std::unique_ptr<Expression>> expressions_; // expression mode
  Schema output_schema_;
};

} // namespace entropy
