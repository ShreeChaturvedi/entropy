/**
 * @file index_selector.cpp
 * @brief Index selection implementation
 */

#include "optimizer/index_selector.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

#include "catalog/catalog.hpp"
#include "parser/expression.hpp"

namespace entropy {

namespace {

// Convert an integer/bigint constant into a B+ tree key. Non-integral constants
// cannot index into the tree and are rejected.
bool const_as_key(const TupleValue &val, BPTreeKey *key) {
  if (val.is_integer()) {
    *key = static_cast<BPTreeKey>(val.as_integer());
    return true;
  }
  if (val.is_bigint()) {
    *key = val.as_bigint();
    return true;
  }
  return false;
}

// Mirror a comparison operator so `const OP column` can be normalized to
// `column FLIP(OP) const` (e.g. `30 < age` becomes `age > 30`).
ComparisonType flip_comparison(ComparisonType op) {
  switch (op) {
  case ComparisonType::LESS_THAN:
    return ComparisonType::GREATER_THAN;
  case ComparisonType::LESS_EQUAL:
    return ComparisonType::GREATER_EQUAL;
  case ComparisonType::GREATER_THAN:
    return ComparisonType::LESS_THAN;
  case ComparisonType::GREATER_EQUAL:
    return ComparisonType::LESS_EQUAL;
  default:
    return op; // EQUAL / NOT_EQUAL are symmetric.
  }
}

// Extract a single `column OP const` bound from a comparison, accepting the
// column on either side (normalizing the operator when it is on the right).
bool extract_single_bound(const ComparisonExpression *comp, column_id_t *column,
                          ComparisonType *op, BPTreeKey *value) {
  auto *left_col = dynamic_cast<const ColumnRefExpression *>(comp->left());
  auto *right_const = dynamic_cast<const ConstantExpression *>(comp->right());
  if (left_col != nullptr && right_const != nullptr) {
    if (!const_as_key(right_const->value(), value)) {
      return false;
    }
    *column = static_cast<column_id_t>(left_col->column_index());
    *op = comp->cmp();
    return true;
  }

  auto *left_const = dynamic_cast<const ConstantExpression *>(comp->left());
  auto *right_col = dynamic_cast<const ColumnRefExpression *>(comp->right());
  if (left_const != nullptr && right_col != nullptr) {
    if (!const_as_key(left_const->value(), value)) {
      return false;
    }
    *column = static_cast<column_id_t>(right_col->column_index());
    *op = flip_comparison(comp->cmp());
    return true;
  }

  return false;
}

// Intersect a new inclusive lower/upper bound with any existing one.
void tighten_min(std::optional<BPTreeKey> *bound, BPTreeKey value) {
  *bound = bound->has_value() ? std::max(**bound, value) : value;
}
void tighten_max(std::optional<BPTreeKey> *bound, BPTreeKey value) {
  *bound = bound->has_value() ? std::min(**bound, value) : value;
}

// Fold one range comparison into inclusive integer [start,end] bounds. Because
// keys are integers, exclusive `<`/`>` are turned into inclusive bounds by
// ±1 (the executor scans [start,end] inclusively). Returns false for
// non-range operators or when the ±1 adjustment would overflow the key domain
// (the caller then falls back to a sequential scan, which stays correct).
bool apply_range_bound(ComparisonType op, BPTreeKey value,
                       std::optional<BPTreeKey> *start,
                       std::optional<BPTreeKey> *end) {
  switch (op) {
  case ComparisonType::LESS_THAN:
    if (value == std::numeric_limits<BPTreeKey>::min()) {
      return false;
    }
    tighten_max(end, value - 1);
    return true;
  case ComparisonType::LESS_EQUAL:
    tighten_max(end, value);
    return true;
  case ComparisonType::GREATER_THAN:
    if (value == std::numeric_limits<BPTreeKey>::max()) {
      return false;
    }
    tighten_min(start, value + 1);
    return true;
  case ComparisonType::GREATER_EQUAL:
    tighten_min(start, value);
    return true;
  default:
    return false;
  }
}

// Collect single-column range bounds from a comparison or a conjunction of
// comparisons on the same column. Returns false for anything else (equality,
// OR/NOT, multiple columns, unsupported constant types).
bool collect_range_bounds(const Expression *expr, bool *have_column,
                          column_id_t *column, std::optional<BPTreeKey> *start,
                          std::optional<BPTreeKey> *end) {
  if (auto *comp = dynamic_cast<const ComparisonExpression *>(expr)) {
    column_id_t col = 0;
    ComparisonType op = ComparisonType::EQUAL;
    BPTreeKey value = 0;
    if (!extract_single_bound(comp, &col, &op, &value)) {
      return false;
    }
    if (*have_column && col != *column) {
      return false; // A single-column index cannot serve two columns.
    }
    *column = col;
    *have_column = true;
    return apply_range_bound(op, value, start, end);
  }

  if (auto *logical = dynamic_cast<const LogicalExpression *>(expr)) {
    if (logical->op() != LogicalOpType::AND) {
      return false;
    }
    return collect_range_bounds(logical->left(), have_column, column, start,
                                end) &&
           collect_range_bounds(logical->right(), have_column, column, start,
                                end);
  }

  return false;
}

} // namespace

IndexSelector::IndexSelector(std::shared_ptr<Catalog> catalog,
                             std::shared_ptr<Statistics> statistics,
                             std::shared_ptr<CostModel> cost_model)
    : catalog_(std::move(catalog)), statistics_(std::move(statistics)),
      cost_model_(std::move(cost_model)) {}

IndexSelection
IndexSelector::select_access_method(oid_t table_oid,
                                    const Expression *predicate) const {
  IndexSelection result;

  // Get table info
  auto *table_info = catalog_->get_table(table_oid);
  if (!table_info) {
    return result; // No table, use seq scan
  }

  // Estimate seq scan cost
  size_t rows = statistics_->table_cardinality(table_oid);
  size_t pages = std::max<size_t>(1, (rows * 100) / 4096);
  result.seq_scan_cost = static_cast<double>(pages) * CostModel::SEQ_PAGE_COST +
                         static_cast<double>(rows) * CostModel::TUPLE_CPU_COST;

  // No predicate = full scan, prefer seq scan
  if (!predicate) {
    return result;
  }

  // Check for point lookup (column = constant)
  column_id_t column;
  BPTreeKey key;
  if (extract_point_lookup(predicate, &column, &key)) {
    auto index_oid = find_index(table_oid, column);
    if (index_oid.has_value()) {
      // Calculate index cost
      double seek_cost =
          std::log2(static_cast<double>(std::max<size_t>(1, rows))) *
          CostModel::RANDOM_PAGE_COST;
      // One random heap fetch per matching tuple (here, a single row).
      double fetch_cost =
          CostModel::RANDOM_PAGE_COST + CostModel::INDEX_TUPLE_COST;
      result.index_cost = seek_cost + fetch_cost;

      // Compare costs
      if (result.index_cost < result.seq_scan_cost) {
        result.use_index = true;
        result.index_oid = *index_oid;
        result.scan_type = IndexScanPlanNode::ScanType::POINT_LOOKUP;
        result.start_key = key;
        result.end_key = key;
      }
    }
  }

  // Check for range scan
  std::optional<BPTreeKey> start, end;
  if (!result.use_index &&
      extract_range_scan(predicate, &column, &start, &end)) {
    auto index_oid = find_index(table_oid, column);
    if (index_oid.has_value()) {
      // Estimate range selectivity from the column's real min/max distribution
      // (falls back to the Selinger 1/3 default without statistics).
      double selectivity =
          statistics_->range_selectivity(table_oid, column, start, end);
      size_t matching_rows =
          static_cast<size_t>(static_cast<double>(rows) * selectivity);

      // Calculate index cost: log(n) seek plus one random heap fetch per
      // matching tuple (unclustered-index assumption).
      double seek_cost =
          std::log2(static_cast<double>(std::max<size_t>(1, rows))) *
          CostModel::RANDOM_PAGE_COST;
      double fetch_cost =
          static_cast<double>(matching_rows) *
          (CostModel::RANDOM_PAGE_COST + CostModel::INDEX_TUPLE_COST);
      result.index_cost = seek_cost + fetch_cost;

      // Use the index purely when it is cheaper than a sequential scan. The
      // previous `selectivity < 0.3` guard compared the fixed 0.33 default
      // against 0.3 -- always false -- so range scans were dead code.
      if (result.index_cost < result.seq_scan_cost) {
        result.use_index = true;
        result.index_oid = *index_oid;
        result.scan_type = IndexScanPlanNode::ScanType::RANGE_SCAN;
        result.start_key = start;
        result.end_key = end;
      }
    }
  }

  return result;
}

std::optional<oid_t> IndexSelector::find_index(oid_t table_oid,
                                               column_id_t column_id) const {
  // Check catalog for index on this column
  auto *index_info = catalog_->get_index_for_column(table_oid, column_id);
  if (index_info) {
    return index_info->oid;
  }
  return std::nullopt;
}

bool IndexSelector::extract_point_lookup(const Expression *predicate,
                                         column_id_t *column,
                                         BPTreeKey *key) const {
  auto *comp = dynamic_cast<const ComparisonExpression *>(predicate);
  if (!comp || comp->cmp() != ComparisonType::EQUAL) {
    return false;
  }
  // Accept `column = const` and `const = column`; equality is symmetric, so the
  // normalized operator stays EQUAL and only the (integer) key is needed.
  ComparisonType op = ComparisonType::EQUAL;
  return extract_single_bound(comp, column, &op, key);
}

bool IndexSelector::extract_range_scan(const Expression *predicate,
                                       column_id_t *column,
                                       std::optional<BPTreeKey> *start,
                                       std::optional<BPTreeKey> *end) const {
  // Bounds are returned as inclusive B+ tree keys so the executor's inclusive
  // [start,end] scan matches the predicate exactly. `<`/`>` are folded to
  // inclusive bounds via ±1 (keys are integers); `column OP const` and
  // `const OP column` are both accepted, as is a conjunction of range
  // comparisons on a single column.
  start->reset();
  end->reset();
  bool have_column = false;
  if (!collect_range_bounds(predicate, &have_column, column, start, end)) {
    return false;
  }
  // At least one usable bound is required to drive an index range scan.
  return have_column && (start->has_value() || end->has_value());
}

} // namespace entropy
