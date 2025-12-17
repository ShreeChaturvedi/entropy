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

  // Extract projected column values
  std::vector<TupleValue> values;
  values.reserve(column_indices_.size());

  for (size_t idx : column_indices_) {
    values.push_back(
        input_tuple->get_value(*input_schema_, static_cast<uint32_t>(idx)));
  }

  // Create output tuple with projected schema
  return Tuple(values, output_schema_);
}

} // namespace entropy
