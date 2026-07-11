/**
 * @file projection.cpp
 * @brief Projection executor implementation
 */

#include "execution/projection.hpp"

namespace entropy {

void ProjectionExecutor::init() {
  if (child_) {
    child_->init();
  }
}

void ProjectionExecutor::build_output_schema() {
  std::vector<Column> columns;
  columns.reserve(column_indices_.size());
  for (size_t idx : column_indices_) {
    columns.push_back(input_schema_->column(idx));
  }
  output_schema_ = Schema(std::move(columns));
}

std::optional<Tuple> ProjectionExecutor::next() {
  auto input_tuple = child_->next();
  if (!input_tuple) {
    return std::nullopt;
  }

  std::vector<TupleValue> values;

  if (!expressions_.empty()) {
    // Expression mode: evaluate each output expression against the input tuple.
    values.reserve(expressions_.size());
    for (const auto &expr : expressions_) {
      values.push_back(expr->evaluate(*input_tuple, *input_schema_));
    }
  } else {
    // Column mode: forward the selected input columns.
    values.reserve(column_indices_.size());
    for (size_t idx : column_indices_) {
      values.push_back(
          input_tuple->get_value(*input_schema_, static_cast<uint32_t>(idx)));
    }
  }

  return Tuple(std::move(values), output_schema_);
}

} // namespace entropy
