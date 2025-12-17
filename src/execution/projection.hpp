#pragma once

/**
 * @file projection.hpp
 * @brief Projection executor - selects specific columns from tuples
 */

#include <memory>
#include <vector>

#include "catalog/schema.hpp"
#include "execution/executor.hpp"

namespace entropy {

/**
 * @brief Projection executor - selects specific columns
 *
 * Takes a child executor and produces tuples with only the
 * specified columns.
 */
class ProjectionExecutor : public Executor {
public:
  /**
   * @brief Construct a projection executor
   * @param ctx Executor context
   * @param child Child executor providing tuples
   * @param input_schema Schema of input tuples
   * @param column_indices Indices of columns to include in output
   */
  ProjectionExecutor(ExecutorContext *ctx, std::unique_ptr<Executor> child,
                     const Schema *input_schema,
                     std::vector<size_t> column_indices)
      : Executor(ctx), child_(std::move(child)), input_schema_(input_schema),
        column_indices_(std::move(column_indices)) {
    build_output_schema();
  }

  /**
   * @brief Initialize the executor
   */
  void init() override;

  /**
   * @brief Get the next projected tuple
   * @return Tuple with only projected columns, or nullopt if done
   */
  [[nodiscard]] std::optional<Tuple> next() override;

  /**
   * @brief Get the output schema
   */
  [[nodiscard]] const Schema &output_schema() const { return output_schema_; }

private:
  void build_output_schema();

  std::unique_ptr<Executor> child_;
  const Schema *input_schema_;
  std::vector<size_t> column_indices_;
  Schema output_schema_;
};

} // namespace entropy
