/**
 * @file nested_loop_join.cpp
 * @brief Nested Loop Join Executor implementation
 */

#include "execution/nested_loop_join.hpp"

namespace entropy {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(
    ExecutorContext *ctx, std::unique_ptr<Executor> left_child,
    std::unique_ptr<Executor> right_child, const Schema *left_schema,
    const Schema *right_schema, const Schema *output_schema, JoinType join_type,
    std::unique_ptr<Expression> condition)
    : Executor(ctx), left_child_(std::move(left_child)),
      right_child_(std::move(right_child)), left_schema_(left_schema),
      right_schema_(right_schema), output_schema_(output_schema),
      join_type_(join_type), condition_(std::move(condition)) {}

void NestedLoopJoinExecutor::init() {
  done_ = false;
  current_left_ = std::nullopt;
  left_has_match_ = false;
  right_index_ = 0;
  right_phase_ = false;
  right_tuples_.clear();
  right_matched_.clear();

  left_child_->init();
  right_child_->init();

  // For RIGHT JOIN, we need to materialize the right side to track matches
  if (join_type_ == JoinType::RIGHT) {
    while (auto tuple = right_child_->next()) {
      right_tuples_.push_back(std::move(*tuple));
      right_matched_.push_back(false);
    }
  }
}

std::optional<Tuple> NestedLoopJoinExecutor::next() {
  if (done_) {
    return std::nullopt;
  }

  // RIGHT JOIN: second phase - emit unmatched right tuples
  if (right_phase_) {
    while (right_index_ < right_tuples_.size()) {
      size_t idx = right_index_++;
      if (!right_matched_[idx]) {
        // Right tuple with NULL left side
        return combine_tuples(nullptr, &right_tuples_[idx]);
      }
    }
    done_ = true;
    return std::nullopt;
  }

  while (true) {
    // Get next left tuple if needed
    if (!current_left_.has_value()) {
      current_left_ = left_child_->next();
      if (!current_left_.has_value()) {
        // No more left tuples
        if (join_type_ == JoinType::RIGHT) {
          // Enter second phase for RIGHT JOIN
          right_phase_ = true;
          right_index_ = 0;
          return next(); // Recurse to handle right phase
        }
        done_ = true;
        return std::nullopt;
      }
      left_has_match_ = false;

      // For non-RIGHT joins, re-init right child for each left tuple
      if (join_type_ != JoinType::RIGHT) {
        right_child_->init();
      } else {
        right_index_ = 0;
      }
    }

    // Scan right side
    if (join_type_ == JoinType::RIGHT) {
      // Use materialized right tuples
      while (right_index_ < right_tuples_.size()) {
        const Tuple &right = right_tuples_[right_index_];
        size_t idx = right_index_++;

        bool matches = (join_type_ == JoinType::CROSS) ||
                       evaluate_condition(*current_left_, right);

        if (matches) {
          left_has_match_ = true;
          right_matched_[idx] = true;
          return combine_tuples(&*current_left_, &right);
        }
      }
    } else {
      // Stream right tuples
      while (auto right = right_child_->next()) {
        bool matches = (join_type_ == JoinType::CROSS) ||
                       evaluate_condition(*current_left_, *right);

        if (matches) {
          left_has_match_ = true;
          return combine_tuples(&*current_left_, &*right);
        }
      }
    }

    // Exhausted right side for current left tuple
    if (join_type_ == JoinType::LEFT && !left_has_match_) {
      // LEFT JOIN: emit left tuple with NULL right side
      auto result = combine_tuples(&*current_left_, nullptr);
      current_left_ = std::nullopt;
      return result;
    }

    // Move to next left tuple
    current_left_ = std::nullopt;
  }
}

bool NestedLoopJoinExecutor::evaluate_condition(const Tuple &left,
                                                const Tuple &right) const {
  if (!condition_) {
    return true; // No condition = always matches (CROSS JOIN)
  }

  // Create combined tuple for evaluation
  Tuple combined = combine_tuples(&left, &right);
  TupleValue result = condition_->evaluate(combined, *output_schema_);

  return result.is_bool() && result.as_bool();
}

Tuple NestedLoopJoinExecutor::combine_tuples(const Tuple *left,
                                             const Tuple *right) const {
  std::vector<TupleValue> values;
  values.reserve(output_schema_->column_count());

  // Add left tuple values (or NULLs)
  for (size_t i = 0; i < left_schema_->column_count(); i++) {
    if (left) {
      values.push_back(
          left->get_value(*left_schema_, static_cast<uint32_t>(i)));
    } else {
      values.push_back(TupleValue::null());
    }
  }

  // Add right tuple values (or NULLs)
  for (size_t i = 0; i < right_schema_->column_count(); i++) {
    if (right) {
      values.push_back(
          right->get_value(*right_schema_, static_cast<uint32_t>(i)));
    } else {
      values.push_back(TupleValue::null());
    }
  }

  return Tuple(std::move(values), *output_schema_);
}

Tuple NestedLoopJoinExecutor::make_null_tuple(const Schema &schema) const {
  std::vector<TupleValue> values;
  values.reserve(schema.column_count());
  for (size_t i = 0; i < schema.column_count(); i++) {
    values.push_back(TupleValue::null());
  }
  return Tuple(std::move(values), schema);
}

} // namespace entropy
