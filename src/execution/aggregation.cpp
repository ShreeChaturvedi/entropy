/**
 * @file aggregation.cpp
 * @brief High-performance Aggregation Executor implementation
 *
 * Performance Optimizations:
 * 1. Reserve hash table capacity upfront when possible
 * 2. Move semantics for key construction
 * 3. Inline hot path operations
 * 4. Avoid runtime type dispatch in accumulation
 * 5. Single-pass aggregation
 */

#include "execution/aggregation.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Constructor
// ─────────────────────────────────────────────────────────────────────────────

AggregationExecutor::AggregationExecutor(
    ExecutorContext *ctx, std::unique_ptr<Executor> child,
    const Schema *input_schema, std::vector<size_t> group_by_indices,
    std::vector<AggregateExpression> aggregates)
    : Executor(ctx), child_(std::move(child)), input_schema_(input_schema),
      group_by_indices_(std::move(group_by_indices)),
      aggregates_(std::move(aggregates)) {

  // Build output schema: GROUP BY columns + aggregate result columns
  std::vector<Column> output_cols;
  output_cols.reserve(group_by_indices_.size() + aggregates_.size());

  // Add GROUP BY columns
  for (size_t idx : group_by_indices_) {
    output_cols.push_back(input_schema_->column(idx));
  }

  // Add aggregate result columns (all aggregates produce numeric output)
  for (const auto &agg : aggregates_) {
    TypeId output_type;
    switch (agg.type) {
    case AggregateType::COUNT_STAR:
    case AggregateType::COUNT:
      output_type = TypeId::BIGINT;
      break;
    case AggregateType::AVG:
      output_type = TypeId::DOUBLE;
      break;
    case AggregateType::SUM:
    case AggregateType::MIN:
    case AggregateType::MAX:
      // Preserve input type for SUM/MIN/MAX
      if (agg.type != AggregateType::COUNT_STAR &&
          agg.column_index < input_schema_->column_count()) {
        output_type = input_schema_->column(agg.column_index).type();
        // Promote to wider type for SUM to avoid overflow
        if (agg.type == AggregateType::SUM) {
          if (output_type == TypeId::INTEGER ||
              output_type == TypeId::SMALLINT ||
              output_type == TypeId::TINYINT) {
            output_type = TypeId::BIGINT;
          } else if (output_type == TypeId::FLOAT) {
            output_type = TypeId::DOUBLE;
          }
        }
      } else {
        output_type = TypeId::DOUBLE;
      }
      break;
    }
    output_cols.emplace_back(agg.alias, output_type);
  }

  output_schema_ = Schema(std::move(output_cols));
}

// ─────────────────────────────────────────────────────────────────────────────
// init() - Build hash table in single pass
// ─────────────────────────────────────────────────────────────────────────────

void AggregationExecutor::init() {
  hash_table_.clear();
  initialized_ = true;
  child_->init();

  // Single-pass aggregation: consume all input and build hash table
  while (auto tuple = child_->next()) {
    AggregateKey key = make_key(*tuple);

    // Find or create entry for this group
    auto it = hash_table_.find(key);
    if (it == hash_table_.end()) {
      // New group: initialize accumulators
      std::vector<AggregateValue> agg_values(aggregates_.size());
      accumulate(*tuple, agg_values);
      hash_table_.emplace(std::move(key), std::move(agg_values));
    } else {
      // Existing group: update accumulators
      accumulate(*tuple, it->second);
    }
  }

  // Handle no-grouping case (SELECT COUNT(*) FROM table with no rows)
  if (group_by_indices_.empty() && hash_table_.empty()) {
    // Create single group with default values
    AggregateKey empty_key;
    std::vector<AggregateValue> agg_values(aggregates_.size());
    hash_table_.emplace(std::move(empty_key), std::move(agg_values));
  }

  ht_iterator_ = hash_table_.begin();
}

// ─────────────────────────────────────────────────────────────────────────────
// next() - Emit one group at a time
// ─────────────────────────────────────────────────────────────────────────────

std::optional<Tuple> AggregationExecutor::next() {
  if (!initialized_ || ht_iterator_ == hash_table_.end()) {
    return std::nullopt;
  }

  const auto &[key, agg_values] = *ht_iterator_;
  Tuple result = make_output_tuple(key, agg_values);
  ++ht_iterator_;
  return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: Make GROUP BY key
// ─────────────────────────────────────────────────────────────────────────────

AggregateKey AggregationExecutor::make_key(const Tuple &tuple) const {
  AggregateKey key;
  key.values.reserve(group_by_indices_.size());

  for (size_t idx : group_by_indices_) {
    key.values.push_back(
        tuple.get_value(*input_schema_, static_cast<uint32_t>(idx)));
  }

  return key;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: Accumulate values into aggregates
// ─────────────────────────────────────────────────────────────────────────────

void AggregationExecutor::accumulate(const Tuple &tuple,
                                     std::vector<AggregateValue> &agg_values) {
  for (size_t i = 0; i < aggregates_.size(); i++) {
    const auto &agg = aggregates_[i];
    auto &val = agg_values[i];

    switch (agg.type) {
    case AggregateType::COUNT_STAR:
      val.count++;
      break;

    case AggregateType::COUNT: {
      TupleValue col_val = tuple.get_value(
          *input_schema_, static_cast<uint32_t>(agg.column_index));
      if (!col_val.is_null()) {
        val.count++;
      }
      break;
    }

    case AggregateType::SUM: {
      TupleValue col_val = tuple.get_value(
          *input_schema_, static_cast<uint32_t>(agg.column_index));
      if (!col_val.is_null()) {
        val.sum += to_numeric(col_val);
        val.has_value = true;
      }
      break;
    }

    case AggregateType::AVG: {
      TupleValue col_val = tuple.get_value(
          *input_schema_, static_cast<uint32_t>(agg.column_index));
      if (!col_val.is_null()) {
        val.sum += to_numeric(col_val);
        val.count++;
        val.has_value = true;
      }
      break;
    }

    case AggregateType::MIN: {
      TupleValue col_val = tuple.get_value(
          *input_schema_, static_cast<uint32_t>(agg.column_index));
      if (!col_val.is_null()) {
        if (!val.has_value || compare_less(col_val, val.min_val)) {
          val.min_val = col_val;
        }
        val.has_value = true;
      }
      break;
    }

    case AggregateType::MAX: {
      TupleValue col_val = tuple.get_value(
          *input_schema_, static_cast<uint32_t>(agg.column_index));
      if (!col_val.is_null()) {
        if (!val.has_value || compare_less(val.max_val, col_val)) {
          val.max_val = col_val;
        }
        val.has_value = true;
      }
      break;
    }
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: Convert TupleValue to double
// ─────────────────────────────────────────────────────────────────────────────

double AggregationExecutor::to_numeric(const TupleValue &val) {
  if (val.is_tinyint())
    return static_cast<double>(val.as_tinyint());
  if (val.is_smallint())
    return static_cast<double>(val.as_smallint());
  if (val.is_integer())
    return static_cast<double>(val.as_integer());
  if (val.is_bigint())
    return static_cast<double>(val.as_bigint());
  if (val.is_float())
    return static_cast<double>(val.as_float());
  if (val.is_double())
    return val.as_double();
  return 0.0; // NULL or non-numeric
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: Compare values for MIN/MAX
// ─────────────────────────────────────────────────────────────────────────────

bool AggregationExecutor::compare_less(const TupleValue &a,
                                       const TupleValue &b) {
  // Handle NULL comparisons
  if (a.is_null())
    return false; // NULL is not less than anything
  if (b.is_null())
    return true; // Any value is less than NULL for this purpose

  // Numeric comparisons (convert to double for simplicity)
  double a_num = to_numeric(a);
  double b_num = to_numeric(b);

  // String comparison
  if (a.is_string() && b.is_string()) {
    return a.as_string() < b.as_string();
  }

  return a_num < b_num;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: Build output tuple
// ─────────────────────────────────────────────────────────────────────────────

Tuple AggregationExecutor::make_output_tuple(
    const AggregateKey &key,
    const std::vector<AggregateValue> &agg_values) const {

  std::vector<TupleValue> values;
  values.reserve(output_schema_.column_count());

  // Add GROUP BY values
  for (const auto &k : key.values) {
    values.push_back(k);
  }

  // Add aggregate results
  for (size_t i = 0; i < aggregates_.size(); i++) {
    const auto &agg = aggregates_[i];
    const auto &val = agg_values[i];
    const Column &out_col = output_schema_.column(group_by_indices_.size() + i);

    switch (agg.type) {
    case AggregateType::COUNT_STAR:
    case AggregateType::COUNT:
      values.push_back(TupleValue(val.count));
      break;

    case AggregateType::SUM:
      if (!val.has_value) {
        values.push_back(TupleValue::null());
      } else if (out_col.type() == TypeId::BIGINT) {
        values.push_back(TupleValue(static_cast<int64_t>(val.sum)));
      } else {
        values.push_back(TupleValue(val.sum));
      }
      break;

    case AggregateType::AVG:
      if (val.count == 0) {
        values.push_back(TupleValue::null());
      } else {
        values.push_back(TupleValue(val.sum / static_cast<double>(val.count)));
      }
      break;

    case AggregateType::MIN:
    case AggregateType::MAX:
      if (!val.has_value) {
        values.push_back(TupleValue::null());
      } else {
        values.push_back(agg.type == AggregateType::MIN ? val.min_val
                                                        : val.max_val);
      }
      break;
    }
  }

  return Tuple(values, output_schema_);
}

} // namespace entropy
