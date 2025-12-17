#pragma once

/**
 * @file hash_join.hpp
 * @brief High-performance Hash Join Executor
 *
 * Design Goals:
 * 1. O(n + m) time complexity for equi-joins (vs O(n*m) for nested loop)
 * 2. Build phase: hash smaller relation into memory hash table
 * 3. Probe phase: stream larger relation, look up matches
 * 4. Support for INNER, LEFT, RIGHT joins
 *
 * Performance Characteristics:
 * - Build: O(n) time, O(n) space for build side
 * - Probe: O(m) time, O(1) space per probe tuple
 * - Optimal for equi-joins (key = key conditions)
 *
 * Memory: Materializes build side; probe side streams
 */

#include <unordered_map>
#include <vector>

#include "execution/executor.hpp"

namespace entropy {

// Forward declaration - JoinType is defined in parser/statement.hpp
enum class JoinType;

/**
 * @brief Hash Join Executor
 *
 * Two-phase hash join:
 * 1. Build: Hash the "build" (typically smaller) table into memory
 * 2. Probe: Stream the "probe" (larger) table, looking up matches
 *
 * Uses multi-map for handling multiple matches per key.
 */
class HashJoinExecutor : public Executor {
public:
  /**
   * @brief Construct hash join executor
   * @param ctx Execution context
   * @param left_child Left (build) side executor
   * @param right_child Right (probe) side executor
   * @param left_schema Schema of left tuples
   * @param right_schema Schema of right tuples
   * @param output_schema Combined output schema
   * @param left_key_index Key column index in left table
   * @param right_key_index Key column index in right table
   * @param join_type Type of join operation
   */
  HashJoinExecutor(ExecutorContext *ctx, std::unique_ptr<Executor> left_child,
                   std::unique_ptr<Executor> right_child,
                   const Schema *left_schema, const Schema *right_schema,
                   const Schema *output_schema, size_t left_key_index,
                   size_t right_key_index, JoinType join_type);

  void init() override;
  std::optional<Tuple> next() override;

  [[nodiscard]] const Schema *output_schema() const noexcept {
    return output_schema_;
  }

private:
  /**
   * @brief Hash function for join keys
   */
  struct KeyHash {
    size_t operator()(const TupleValue &key) const noexcept;
  };

  /**
   * @brief Equality for join keys
   */
  struct KeyEqual {
    bool operator()(const TupleValue &a, const TupleValue &b) const noexcept;
  };

  /**
   * @brief Combine left and right tuples into output
   */
  Tuple combine_tuples(const Tuple *left, const Tuple *right) const;

  std::unique_ptr<Executor> left_child_;
  std::unique_ptr<Executor> right_child_;
  const Schema *left_schema_;
  const Schema *right_schema_;
  const Schema *output_schema_;
  size_t left_key_index_;
  size_t right_key_index_;
  JoinType join_type_;

  // Hash table: key -> list of matching tuples from build side
  using HashTable =
      std::unordered_multimap<TupleValue, Tuple, KeyHash, KeyEqual>;
  HashTable hash_table_;

  // Probe state
  std::optional<Tuple> current_probe_;
  HashTable::iterator match_begin_;
  HashTable::iterator match_end_;
  bool probe_had_match_ = false;

  // For RIGHT JOIN: track which build tuples matched
  std::vector<bool> build_matched_;
  std::vector<Tuple> build_tuples_; // For RIGHT JOIN unmatched emission
  size_t right_phase_index_ = 0;
  bool in_right_phase_ = false;

  bool initialized_ = false;
};

} // namespace entropy
