#pragma once

/**
 * @file aggregation.hpp
 * @brief High-performance Aggregation Executor
 *
 * Design Goals:
 * 1. Hash-based aggregation for O(1) amortized group lookup
 * 2. Single-pass accumulation (no sorting required)
 * 3. Incremental aggregates (constant memory per group)
 * 4. Efficient key hashing with FNV-1a variant
 *
 * Supported Aggregates:
 * - COUNT(*): Uses count accumulator
 * - COUNT(col): Counts non-NULL values
 * - SUM(col): Numeric sum with type promotion
 * - AVG(col): Sum + count, computed at output time
 * - MIN(col): Tracks minimum value
 * - MAX(col): Tracks maximum value
 */

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "execution/executor.hpp"
#include "parser/expression.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Aggregate Type
// ─────────────────────────────────────────────────────────────────────────────

enum class AggregateType {
  COUNT_STAR, // COUNT(*)
  COUNT,      // COUNT(column) - non-NULL only
  SUM,
  AVG,
  MIN,
  MAX,
};

/**
 * @brief Definition of an aggregate function
 */
struct AggregateExpression {
  AggregateType type;
  size_t column_index; // Index in input schema (ignored for COUNT_STAR)
  std::string alias;   // Output column name

  AggregateExpression(AggregateType t, size_t col_idx, std::string name)
      : type(t), column_index(col_idx), alias(std::move(name)) {}

  // Convenience factory methods
  static AggregateExpression count_star(const std::string &name = "count") {
    return {AggregateType::COUNT_STAR, 0, name};
  }
  static AggregateExpression count(size_t col,
                                   const std::string &name = "count") {
    return {AggregateType::COUNT, col, name};
  }
  static AggregateExpression sum(size_t col, const std::string &name = "sum") {
    return {AggregateType::SUM, col, name};
  }
  static AggregateExpression avg(size_t col, const std::string &name = "avg") {
    return {AggregateType::AVG, col, name};
  }
  static AggregateExpression min(size_t col, const std::string &name = "min") {
    return {AggregateType::MIN, col, name};
  }
  static AggregateExpression max(size_t col, const std::string &name = "max") {
    return {AggregateType::MAX, col, name};
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// Aggregate Key (for GROUP BY)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Key for hash-based grouping
 *
 * Uses vector of TupleValue for composite GROUP BY keys.
 * Implements custom hash for efficient lookups.
 */
struct AggregateKey {
  std::vector<TupleValue> values;

  bool operator==(const AggregateKey &other) const {
    if (values.size() != other.values.size())
      return false;
    for (size_t i = 0; i < values.size(); i++) {
      if (values[i] != other.values[i])
        return false;
    }
    return true;
  }
};

/**
 * @brief High-performance hash function for AggregateKey
 *
 * Uses FNV-1a inspired combining for good distribution.
 * Handles all TupleValue types efficiently.
 */
struct AggregateKeyHash {
  size_t operator()(const AggregateKey &key) const noexcept {
    // FNV-1a offset basis
    size_t hash = 14695981039346656037ULL;
    constexpr size_t fnv_prime = 1099511628211ULL;

    for (const auto &val : key.values) {
      size_t val_hash = hash_value(val);
      hash ^= val_hash;
      hash *= fnv_prime;
    }
    return hash;
  }

private:
  static size_t hash_value(const TupleValue &val) noexcept {
    if (val.is_null())
      return 0;
    if (val.is_bool())
      return std::hash<bool>{}(val.as_bool());
    if (val.is_tinyint())
      return std::hash<int8_t>{}(val.as_tinyint());
    if (val.is_smallint())
      return std::hash<int16_t>{}(val.as_smallint());
    if (val.is_integer())
      return std::hash<int32_t>{}(val.as_integer());
    if (val.is_bigint())
      return std::hash<int64_t>{}(val.as_bigint());
    if (val.is_float())
      return std::hash<float>{}(val.as_float());
    if (val.is_double())
      return std::hash<double>{}(val.as_double());
    if (val.is_string())
      return std::hash<std::string>{}(val.as_string());
    return 0;
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// Aggregate Accumulator
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Accumulator for incremental aggregate computation
 *
 * Each group has one AggregateValue per aggregate function.
 * Uses running accumulators to minimize memory and enable single-pass.
 */
struct AggregateValue {
  int64_t count = 0;      // For COUNT and AVG denominator
  double sum = 0.0;       // For SUM and AVG numerator
  TupleValue min_val;     // For MIN
  TupleValue max_val;     // For MAX
  bool has_value = false; // Has seen at least one non-NULL value

  void reset() {
    count = 0;
    sum = 0.0;
    min_val = TupleValue::null();
    max_val = TupleValue::null();
    has_value = false;
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation Executor
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Hash-based Aggregation Executor
 *
 * Performs aggregation with optional GROUP BY in a single pass.
 * Uses hash table for O(1) amortized group lookup.
 *
 * Execution Model:
 * - init(): Consume all input, build hash table
 * - next(): Iterate over hash table entries, emit one row per group
 *
 * Performance Characteristics:
 * - O(n) input scan
 * - O(g) memory where g = number of groups
 * - O(1) amortized per-tuple processing
 */
class AggregationExecutor : public Executor {
public:
  /**
   * @brief Construct aggregation executor
   * @param ctx Execution context
   * @param child Child executor providing input tuples
   * @param input_schema Schema of input tuples
   * @param group_by_indices Column indices for GROUP BY (empty = no grouping)
   * @param aggregates Aggregate functions to compute
   */
  AggregationExecutor(ExecutorContext *ctx, std::unique_ptr<Executor> child,
                      const Schema *input_schema,
                      std::vector<size_t> group_by_indices,
                      std::vector<AggregateExpression> aggregates);

  void init() override;
  std::optional<Tuple> next() override;

  /**
   * @brief Get output schema
   * GROUP BY columns + aggregate result columns
   */
  [[nodiscard]] const Schema &output_schema() const noexcept {
    return output_schema_;
  }

private:
  /**
   * @brief Extract GROUP BY key from tuple
   */
  AggregateKey make_key(const Tuple &tuple) const;

  /**
   * @brief Accumulate tuple into aggregate values
   */
  void accumulate(const Tuple &tuple, std::vector<AggregateValue> &agg_values);

  /**
   * @brief Convert numeric TupleValue to double for aggregation
   */
  static double to_numeric(const TupleValue &val);

  /**
   * @brief Compare two values for MIN/MAX
   * Returns true if a < b
   */
  static bool compare_less(const TupleValue &a, const TupleValue &b);

  /**
   * @brief Build output tuple from key and aggregates
   */
  Tuple make_output_tuple(const AggregateKey &key,
                          const std::vector<AggregateValue> &agg_values) const;

  std::unique_ptr<Executor> child_;
  const Schema *input_schema_;
  std::vector<size_t> group_by_indices_;
  std::vector<AggregateExpression> aggregates_;
  Schema output_schema_;

  // Hash table: GROUP BY key -> aggregate accumulators
  using HashTable =
      std::unordered_map<AggregateKey, std::vector<AggregateValue>,
                         AggregateKeyHash>;
  HashTable hash_table_;
  HashTable::iterator ht_iterator_;
  bool initialized_ = false;
};

} // namespace entropy
