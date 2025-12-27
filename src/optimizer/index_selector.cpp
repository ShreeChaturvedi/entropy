/**
 * @file index_selector.cpp
 * @brief Index selection implementation
 */

#include "optimizer/index_selector.hpp"

#include <algorithm>
#include <cmath>

#include "catalog/catalog.hpp"
#include "parser/expression.hpp"

namespace entropy {

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
      double fetch_cost = CostModel::INDEX_TUPLE_COST;
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
      // Estimate range selectivity
      double selectivity = Statistics::RANGE_SELECTIVITY;
      size_t matching_rows =
          static_cast<size_t>(static_cast<double>(rows) * selectivity);

      // Calculate index cost
      double seek_cost =
          std::log2(static_cast<double>(std::max<size_t>(1, rows))) *
          CostModel::RANDOM_PAGE_COST;
      double fetch_cost =
          static_cast<double>(matching_rows) * CostModel::INDEX_TUPLE_COST;
      result.index_cost = seek_cost + fetch_cost;

      // Index is beneficial if selectivity is low
      if (selectivity < 0.3 && result.index_cost < result.seq_scan_cost) {
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

  // Check for column = constant pattern
  auto *left_col = dynamic_cast<const ColumnRefExpression *>(comp->left());
  auto *right_const = dynamic_cast<const ConstantExpression *>(comp->right());

  if (left_col && right_const) {
    *column = static_cast<column_id_t>(left_col->column_index());
    // Extract key value
    const auto &val = right_const->value();
    if (val.is_integer()) {
      *key = static_cast<BPTreeKey>(val.as_integer());
      return true;
    }
    if (val.is_bigint()) {
      *key = val.as_bigint();
      return true;
    }
  }

  // Check for constant = column pattern
  auto *left_const = dynamic_cast<const ConstantExpression *>(comp->left());
  auto *right_col = dynamic_cast<const ColumnRefExpression *>(comp->right());

  if (left_const && right_col) {
    *column = static_cast<column_id_t>(right_col->column_index());
    const auto &val = left_const->value();
    if (val.is_integer()) {
      *key = static_cast<BPTreeKey>(val.as_integer());
      return true;
    }
    if (val.is_bigint()) {
      *key = val.as_bigint();
      return true;
    }
  }

  return false;
}

bool IndexSelector::extract_range_scan(const Expression *predicate,
                                       column_id_t *column,
                                       std::optional<BPTreeKey> *start,
                                       std::optional<BPTreeKey> *end) const {
  auto *comp = dynamic_cast<const ComparisonExpression *>(predicate);
  if (!comp)
    return false;

  auto *left_col = dynamic_cast<const ColumnRefExpression *>(comp->left());
  auto *right_const = dynamic_cast<const ConstantExpression *>(comp->right());

  if (!left_col || !right_const)
    return false;

  *column = static_cast<column_id_t>(left_col->column_index());

  BPTreeKey value = 0;
  const auto &val = right_const->value();
  if (val.is_integer()) {
    value = static_cast<BPTreeKey>(val.as_integer());
  } else if (val.is_bigint()) {
    value = val.as_bigint();
  } else {
    return false;
  }

  switch (comp->cmp()) {
  case ComparisonType::LESS_THAN:
  case ComparisonType::LESS_EQUAL:
    *end = value;
    return true;
  case ComparisonType::GREATER_THAN:
  case ComparisonType::GREATER_EQUAL:
    *start = value;
    return true;
  default:
    return false;
  }
}

} // namespace entropy
