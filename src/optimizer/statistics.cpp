/**
 * @file statistics.cpp
 * @brief Statistics collection and estimation implementation
 */

#include "optimizer/statistics.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <functional>
#include <optional>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "catalog/catalog.hpp"
#include "storage/table_heap.hpp"

namespace entropy {

namespace {

// Hash for TupleValue so exact distinct values can be counted with a set.
// NULLs are never inserted, so the monostate branch is unreachable in practice.
struct TupleValueHash {
  size_t operator()(const TupleValue &v) const {
    return std::visit(
        [](const auto &x) -> size_t {
          using T = std::decay_t<decltype(x)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
            return 0;
          } else {
            return std::hash<T>{}(x);
          }
        },
        v.value());
  }
};

// Extract a numeric value for ordering (min/max). Returns false for
// non-numeric types (e.g. VARCHAR), which are ordered separately.
bool numeric_value(const TupleValue &v, double *out) {
  if (v.is_bool()) {
    *out = v.as_bool() ? 1.0 : 0.0;
  } else if (v.is_tinyint()) {
    *out = static_cast<double>(v.as_tinyint());
  } else if (v.is_smallint()) {
    *out = static_cast<double>(v.as_smallint());
  } else if (v.is_integer()) {
    *out = static_cast<double>(v.as_integer());
  } else if (v.is_bigint()) {
    *out = static_cast<double>(v.as_bigint());
  } else if (v.is_float()) {
    *out = static_cast<double>(v.as_float());
  } else if (v.is_double()) {
    *out = v.as_double();
  } else {
    return false;
  }
  return true;
}

// Order two non-null values (numeric by magnitude, strings lexicographically).
bool value_less(const TupleValue &a, const TupleValue &b) {
  double da = 0.0;
  double db = 0.0;
  if (numeric_value(a, &da) && numeric_value(b, &db)) {
    return da < db;
  }
  if (a.is_string() && b.is_string()) {
    return a.as_string() < b.as_string();
  }
  return false; // Incomparable: leave the current extremum in place.
}

} // namespace

Statistics::Statistics(std::shared_ptr<Catalog> catalog)
    : catalog_(std::move(catalog)) {}

size_t Statistics::table_cardinality(oid_t table_oid) const {
  auto it = table_stats_.find(table_oid);
  if (it != table_stats_.end()) {
    return it->second.row_count;
  }

  // If no stats, do a quick count from catalog
  auto *table_info = catalog_->get_table(table_oid);
  if (!table_info) {
    return 0;
  }

  // Quick estimate: count tuples
  size_t count = 0;
  for (auto tuple_it = table_info->table_heap->begin();
       tuple_it != table_info->table_heap->end(); ++tuple_it) {
    ++count;
  }
  return count;
}

double Statistics::column_selectivity(oid_t table_oid,
                                      column_id_t column_id) const {
  auto table_it = table_stats_.find(table_oid);
  if (table_it == table_stats_.end()) {
    return DEFAULT_SELECTIVITY;
  }

  auto col_it = table_it->second.columns.find(column_id);
  if (col_it == table_it->second.columns.end()) {
    return DEFAULT_SELECTIVITY;
  }

  // Selectivity = 1 / distinct_values for equality predicates
  if (col_it->second.distinct_values > 0) {
    return 1.0 / col_it->second.distinct_values;
  }

  return DEFAULT_SELECTIVITY;
}

double Statistics::estimate_selectivity([[maybe_unused]] oid_t table_oid,
                                        const Expression *predicate) const {
  if (!predicate) {
    return 1.0; // No predicate = all rows
  }

  // Handle comparison expressions
  if (auto *comp = dynamic_cast<const ComparisonExpression *>(predicate)) {
    switch (comp->cmp()) {
    case ComparisonType::EQUAL:
      return EQUALITY_SELECTIVITY;
    case ComparisonType::LESS_THAN:
    case ComparisonType::LESS_EQUAL:
    case ComparisonType::GREATER_THAN:
    case ComparisonType::GREATER_EQUAL:
      return RANGE_SELECTIVITY;
    case ComparisonType::NOT_EQUAL:
      return 1.0 - EQUALITY_SELECTIVITY;
    default:
      return DEFAULT_SELECTIVITY;
    }
  }

  // Handle logical expressions
  if (auto *logical = dynamic_cast<const LogicalExpression *>(predicate)) {
    // For simplicity, use default selectivity for logical expressions
    // Full implementation would recursively estimate
    switch (logical->op()) {
    case LogicalOpType::AND:
      return DEFAULT_SELECTIVITY * DEFAULT_SELECTIVITY;
    case LogicalOpType::OR:
      return 2 * DEFAULT_SELECTIVITY -
             (DEFAULT_SELECTIVITY * DEFAULT_SELECTIVITY);
    case LogicalOpType::NOT:
      return 1.0 - DEFAULT_SELECTIVITY;
    default:
      return DEFAULT_SELECTIVITY;
    }
  }

  return DEFAULT_SELECTIVITY;
}

void Statistics::collect_statistics(oid_t table_oid) {
  auto *table_info = catalog_->get_table(table_oid);
  if (!table_info) {
    return;
  }

  TableStatistics stats;
  const Schema &schema = table_info->schema;

  // Initialize column stats
  for (size_t i = 0; i < schema.column_count(); ++i) {
    stats.columns[static_cast<column_id_t>(i)] = ColumnStatistics{};
  }

  // Single pass: count rows, count true distinct non-null values, and track
  // per-column min/max. Sized to the actual column count so tables with any
  // number of columns are handled (previously fixed 32-element stack arrays
  // overflowed for >32).
  const size_t col_count = schema.column_count();
  std::vector<size_t> null_counts(col_count, 0);
  std::vector<std::unordered_set<TupleValue, TupleValueHash>> distinct_sets(
      col_count);
  std::vector<std::optional<TupleValue>> col_min(col_count);
  std::vector<std::optional<TupleValue>> col_max(col_count);

  for (auto it = table_info->table_heap->begin();
       it != table_info->table_heap->end(); ++it) {
    Tuple tuple = *it;
    ++stats.row_count;

    for (size_t i = 0; i < col_count; ++i) {
      TupleValue val = tuple.get_value(schema, static_cast<uint32_t>(i));

      if (val.is_null()) {
        ++null_counts[i];
        continue;
      }
      // Count exact distinct values instead of assuming ~90% are unique.
      distinct_sets[i].insert(val);
      if (!col_min[i] || value_less(val, *col_min[i])) {
        col_min[i] = val;
      }
      if (!col_max[i] || value_less(*col_max[i], val)) {
        col_max[i] = val;
      }
    }
  }

  // Finalize column statistics
  for (size_t i = 0; i < col_count; ++i) {
    auto &col_stats = stats.columns[static_cast<column_id_t>(i)];
    // NDV = number of distinct non-null values actually observed.
    col_stats.distinct_values = static_cast<double>(distinct_sets[i].size());
    col_stats.min_value = col_min[i];
    col_stats.max_value = col_max[i];
    if (stats.row_count > 0) {
      col_stats.null_fraction =
          static_cast<double>(null_counts[i]) /
          static_cast<double>(stats.row_count);
    }
  }

  // Estimate page count (rough: 4KB pages, ~100 bytes per row avg)
  stats.page_count = std::max<size_t>(1, (stats.row_count * 100) / 4096);

  table_stats_[table_oid] = std::move(stats);
}

void Statistics::on_table_created(oid_t table_oid) {
  TableStatistics stats;
  stats.row_count = 0;
  stats.page_count = 0;
  table_stats_[table_oid] = std::move(stats);
}

void Statistics::on_table_dropped(oid_t table_oid) {
  table_stats_.erase(table_oid);
}

void Statistics::on_rows_inserted(oid_t table_oid, size_t rows) {
  if (rows == 0) {
    return;
  }
  auto &stats = table_stats_[table_oid];
  stats.row_count += rows;
  if (stats.row_count == 0) {
    stats.page_count = 0;
  } else {
    stats.page_count = std::max<size_t>(1, (stats.row_count * 100) / 4096);
  }
}

void Statistics::on_rows_deleted(oid_t table_oid, size_t rows) {
  auto it = table_stats_.find(table_oid);
  if (it == table_stats_.end()) {
    return;
  }
  if (rows >= it->second.row_count) {
    it->second.row_count = 0;
  } else {
    it->second.row_count -= rows;
  }

  if (it->second.row_count == 0) {
    it->second.page_count = 0;
  } else {
    it->second.page_count =
        std::max<size_t>(1, (it->second.row_count * 100) / 4096);
  }
}

const TableStatistics *Statistics::get_table_stats(oid_t table_oid) const {
  auto it = table_stats_.find(table_oid);
  return (it != table_stats_.end()) ? &it->second : nullptr;
}

} // namespace entropy
