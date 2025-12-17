#pragma once

/**
 * @file nested_loop_join.hpp
 * @brief Nested Loop Join Executor
 *
 * Implements the classic nested loop join algorithm.
 * For each tuple from the outer (left) table, scans all tuples
 * from the inner (right) table looking for matches.
 *
 * Supports:
 * - INNER JOIN: Only matching tuples
 * - LEFT JOIN: All left tuples + matching right tuples (NULLs for non-matches)
 * - RIGHT JOIN: All right tuples + matching left tuples (NULLs for non-matches)
 * - CROSS JOIN: Cartesian product (no condition)
 */

#include "execution/executor.hpp"
#include "parser/expression.hpp"
#include "parser/statement.hpp"

namespace entropy {

/**
 * @brief Nested Loop Join Executor
 *
 * Joins two child executors using the nested loop algorithm.
 * The outer (left) child is iterated once; the inner (right)
 * child is re-initialized and iterated for each outer tuple.
 */
class NestedLoopJoinExecutor : public Executor {
public:
  /**
   * @brief Construct a nested loop join executor
   * @param ctx Execution context
   * @param left_child Left (outer) child executor
   * @param right_child Right (inner) child executor
   * @param left_schema Schema of left child tuples
   * @param right_schema Schema of right child tuples
   * @param output_schema Schema of output tuples (combined)
   * @param join_type Type of join (INNER, LEFT, RIGHT, CROSS)
   * @param condition Join condition (null for CROSS JOIN)
   */
  NestedLoopJoinExecutor(ExecutorContext *ctx,
                         std::unique_ptr<Executor> left_child,
                         std::unique_ptr<Executor> right_child,
                         const Schema *left_schema, const Schema *right_schema,
                         const Schema *output_schema, JoinType join_type,
                         std::unique_ptr<Expression> condition);

  void init() override;
  std::optional<Tuple> next() override;

  [[nodiscard]] const Schema *output_schema() const noexcept {
    return output_schema_;
  }

private:
  /**
   * @brief Combine left and right tuples into output tuple
   */
  Tuple combine_tuples(const Tuple *left, const Tuple *right) const;

  /**
   * @brief Create a tuple with all NULL values for a schema
   */
  Tuple make_null_tuple(const Schema &schema) const;

  /**
   * @brief Evaluate join condition
   */
  bool evaluate_condition(const Tuple &left, const Tuple &right) const;

  std::unique_ptr<Executor> left_child_;
  std::unique_ptr<Executor> right_child_;
  const Schema *left_schema_;
  const Schema *right_schema_;
  const Schema *output_schema_;
  JoinType join_type_;
  std::unique_ptr<Expression> condition_;

  // State for iteration
  std::optional<Tuple> current_left_;
  bool left_has_match_ = false; // For LEFT JOIN: did current left find a match?
  bool done_ = false;

  // For RIGHT JOIN: need to track which right tuples matched
  std::vector<Tuple> right_tuples_; // Materialized right side
  std::vector<bool> right_matched_; // Which right tuples matched
  size_t right_index_ = 0;
  bool right_phase_ = false; // Are we emitting unmatched right tuples?
};

} // namespace entropy
