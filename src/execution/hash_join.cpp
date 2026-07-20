/**
 * @file hash_join.cpp
 * @brief High-performance Hash Join implementation
 *
 * Two-phase approach:
 * 1. Build phase: materialize left (build) side into hash table
 * 2. Probe phase: stream right (probe) side, look up matches
 *
 * Performance: O(n + m) for n build tuples, m probe tuples
 *
 * Outer-join semantics (ANSI / matching NestedLoopJoinExecutor):
 * - LEFT: preserve all build (left) rows; NULL-fill probe columns when unmatched
 * - RIGHT: preserve all probe (right) rows; NULL-fill build columns when unmatched
 *
 * NULL join keys never participate in equality matches (SQL three-valued logic).
 */

#include "execution/hash_join.hpp"
#include "parser/statement.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Key Hash - FNV-1a variant for TupleValue
// ─────────────────────────────────────────────────────────────────────────────

size_t
HashJoinExecutor::KeyHash::operator()(const TupleValue &key) const noexcept {
  if (key.is_null())
    return 0;
  if (key.is_bool())
    return std::hash<bool>{}(key.as_bool());
  if (key.is_tinyint())
    return std::hash<int8_t>{}(key.as_tinyint());
  if (key.is_smallint())
    return std::hash<int16_t>{}(key.as_smallint());
  if (key.is_integer())
    return std::hash<int32_t>{}(key.as_integer());
  if (key.is_bigint())
    return std::hash<int64_t>{}(key.as_bigint());
  if (key.is_float())
    return std::hash<float>{}(key.as_float());
  if (key.is_double())
    return std::hash<double>{}(key.as_double());
  if (key.is_string())
    return std::hash<std::string>{}(key.as_string());
  return 0;
}

// ─────────────────────────────────────────────────────────────────────────────
// Key Equality
// ─────────────────────────────────────────────────────────────────────────────

bool HashJoinExecutor::KeyEqual::operator()(
    const TupleValue &a, const TupleValue &b) const noexcept {
  // SQL: NULL never equals NULL (or any other value) in join predicates
  if (a.is_null() || b.is_null())
    return false;
  return a == b;
}

// ─────────────────────────────────────────────────────────────────────────────
// Constructor
// ─────────────────────────────────────────────────────────────────────────────

HashJoinExecutor::HashJoinExecutor(
    ExecutorContext *ctx, std::unique_ptr<Executor> left_child,
    std::unique_ptr<Executor> right_child, const Schema *left_schema,
    const Schema *right_schema, const Schema *output_schema,
    size_t left_key_index, size_t right_key_index, JoinType join_type)
    : Executor(ctx), left_child_(std::move(left_child)),
      right_child_(std::move(right_child)), left_schema_(left_schema),
      right_schema_(right_schema), output_schema_(output_schema),
      left_key_index_(left_key_index), right_key_index_(right_key_index),
      join_type_(join_type) {}

// ─────────────────────────────────────────────────────────────────────────────
// init() - Build phase
// ─────────────────────────────────────────────────────────────────────────────

void HashJoinExecutor::init() {
  hash_table_.clear();
  build_matched_.clear();
  build_tuples_.clear();
  current_probe_ = std::nullopt;
  probe_had_match_ = false;
  unmatched_build_index_ = 0;
  in_unmatched_build_phase_ = false;
  initialized_ = true;

  left_child_->init();
  right_child_->init();

  // Build phase: materialize left (build) side; hash non-NULL keys
  while (auto tuple = left_child_->next()) {
    size_t idx = build_tuples_.size();
    build_tuples_.push_back(*tuple);

    if (join_type_ == JoinType::LEFT) {
      build_matched_.push_back(false);
    }

    TupleValue key =
        tuple->get_value(*left_schema_, static_cast<uint32_t>(left_key_index_));
    if (!key.is_null()) {
      hash_table_.emplace(key, idx);
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// next() - Probe phase
// ─────────────────────────────────────────────────────────────────────────────

std::optional<Tuple> HashJoinExecutor::next() {
  if (!initialized_)
    return std::nullopt;

  // LEFT JOIN: second phase - emit unmatched build tuples (NULL-filled right)
  if (in_unmatched_build_phase_) {
    while (unmatched_build_index_ < build_tuples_.size()) {
      size_t idx = unmatched_build_index_++;
      if (!build_matched_[idx]) {
        return combine_tuples(&build_tuples_[idx], nullptr);
      }
    }
    return std::nullopt;
  }

  while (true) {
    // If we have remaining matches for current probe tuple, emit them
    if (current_probe_.has_value() && match_begin_ != match_end_) {
      size_t build_idx = match_begin_->second;

      if (join_type_ == JoinType::LEFT) {
        build_matched_[build_idx] = true;
      }

      probe_had_match_ = true;
      auto result =
          combine_tuples(&build_tuples_[build_idx], &*current_probe_);
      ++match_begin_;
      return result;
    }

    // RIGHT JOIN: emit probe tuple with NULL left if no matches
    if (join_type_ == JoinType::RIGHT && current_probe_.has_value() &&
        !probe_had_match_) {
      auto result = combine_tuples(nullptr, &*current_probe_);
      current_probe_ = std::nullopt;
      return result;
    }

    // Get next probe tuple
    current_probe_ = right_child_->next();
    if (!current_probe_.has_value()) {
      // No more probe tuples
      if (join_type_ == JoinType::LEFT) {
        in_unmatched_build_phase_ = true;
        unmatched_build_index_ = 0;
        return next(); // Recurse to handle unmatched build phase
      }
      return std::nullopt;
    }

    // Look up in hash table (NULL probe keys never match)
    probe_had_match_ = false;
    TupleValue probe_key = current_probe_->get_value(
        *right_schema_, static_cast<uint32_t>(right_key_index_));
    if (probe_key.is_null()) {
      match_begin_ = hash_table_.end();
      match_end_ = hash_table_.end();
    } else {
      auto range = hash_table_.equal_range(probe_key);
      match_begin_ = range.first;
      match_end_ = range.second;
    }

    // For INNER join with no matches, just continue to next probe tuple
    if (join_type_ == JoinType::INNER && match_begin_ == match_end_) {
      current_probe_ = std::nullopt;
      continue;
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Combine tuples
// ─────────────────────────────────────────────────────────────────────────────

Tuple HashJoinExecutor::combine_tuples(const Tuple *left,
                                       const Tuple *right) const {
  std::vector<TupleValue> values;
  values.reserve(output_schema_->column_count());

  // Add left (build) values
  for (size_t i = 0; i < left_schema_->column_count(); i++) {
    if (left) {
      values.push_back(
          left->get_value(*left_schema_, static_cast<uint32_t>(i)));
    } else {
      values.push_back(TupleValue::null());
    }
  }

  // Add right (probe) values
  for (size_t i = 0; i < right_schema_->column_count(); i++) {
    if (right) {
      values.push_back(
          right->get_value(*right_schema_, static_cast<uint32_t>(i)));
    } else {
      values.push_back(TupleValue::null());
    }
  }

  return Tuple(values, *output_schema_);
}

} // namespace entropy
