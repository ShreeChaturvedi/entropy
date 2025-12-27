/**
 * @file cost_model.cpp
 * @brief Cost estimation implementation
 */

#include "optimizer/cost_model.hpp"

#include <algorithm>
#include <cmath>

namespace entropy {

CostModel::CostModel(std::shared_ptr<Statistics> statistics)
    : statistics_(std::move(statistics)) {}

double CostModel::estimate_cost(const PlanNode *plan) const {
  if (!plan)
    return 0.0;

  double cost = 0.0;

  switch (plan->type()) {
  case PlanNodeType::SEQ_SCAN:
    cost = cost_seq_scan(static_cast<const SeqScanPlanNode *>(plan));
    break;
  case PlanNodeType::INDEX_SCAN:
    cost = cost_index_scan(static_cast<const IndexScanPlanNode *>(plan));
    break;
  case PlanNodeType::FILTER:
    cost = cost_filter(static_cast<const FilterPlanNode *>(plan));
    break;
  case PlanNodeType::PROJECTION:
    cost = cost_projection(static_cast<const ProjectionPlanNode *>(plan));
    break;
  case PlanNodeType::SORT:
    cost = cost_sort(static_cast<const SortPlanNode *>(plan));
    break;
  case PlanNodeType::LIMIT:
    cost = cost_limit(static_cast<const LimitPlanNode *>(plan));
    break;
  case PlanNodeType::NESTED_LOOP_JOIN:
    cost = cost_nested_loop_join(
        static_cast<const NestedLoopJoinPlanNode *>(plan));
    break;
  case PlanNodeType::HASH_JOIN:
    cost = cost_hash_join(static_cast<const HashJoinPlanNode *>(plan));
    break;
  case PlanNodeType::AGGREGATION:
    cost = cost_aggregation(static_cast<const AggregationPlanNode *>(plan));
    break;
  default:
    cost = children_cost(plan);
    break;
  }

  return cost + children_cost(plan);
}

size_t CostModel::estimate_cardinality(const PlanNode *plan) const {
  if (!plan)
    return 0;

  // Get child cardinality
  size_t child_card = 0;
  if (!plan->children().empty()) {
    child_card = estimate_cardinality(plan->children()[0].get());
  }

  switch (plan->type()) {
  case PlanNodeType::SEQ_SCAN: {
    auto *scan = static_cast<const SeqScanPlanNode *>(plan);
    size_t base = statistics_->table_cardinality(scan->table_oid());
    if (scan->predicate()) {
      double sel = statistics_->estimate_selectivity(scan->table_oid(),
                                                     scan->predicate());
      return static_cast<size_t>(static_cast<double>(base) * sel);
    }
    return base;
  }

  case PlanNodeType::INDEX_SCAN: {
    auto *scan = static_cast<const IndexScanPlanNode *>(plan);
    size_t base = statistics_->table_cardinality(scan->table_oid());
    switch (scan->scan_type()) {
    case IndexScanPlanNode::ScanType::POINT_LOOKUP:
      return 1; // Single row
    case IndexScanPlanNode::ScanType::RANGE_SCAN:
      return static_cast<size_t>(static_cast<double>(base) *
                                 Statistics::RANGE_SELECTIVITY);
    case IndexScanPlanNode::ScanType::FULL_SCAN:
      return base;
    }
    return base;
  }

  case PlanNodeType::FILTER: {
    // Estimate based on predicate selectivity
    double sel = Statistics::DEFAULT_SELECTIVITY;
    return static_cast<size_t>(static_cast<double>(child_card) * sel);
  }

  case PlanNodeType::LIMIT: {
    auto *limit = static_cast<const LimitPlanNode *>(plan);
    if (limit->limit()) {
      return std::min(child_card, *limit->limit());
    }
    return child_card;
  }

  case PlanNodeType::NESTED_LOOP_JOIN:
  case PlanNodeType::HASH_JOIN: {
    // Estimate as product with selectivity
    if (plan->children().size() >= 2) {
      size_t left_card = estimate_cardinality(plan->children()[0].get());
      size_t right_card = estimate_cardinality(plan->children()[1].get());
      return static_cast<size_t>(static_cast<double>(left_card) *
                                 static_cast<double>(right_card) *
                                 Statistics::EQUALITY_SELECTIVITY);
    }
    return child_card;
  }

  default:
    return child_card;
  }
}

double CostModel::cost_seq_scan(const SeqScanPlanNode *node) const {
  size_t rows = statistics_->table_cardinality(node->table_oid());
  // Estimate pages: ~100 bytes per row, 4KB pages
  size_t pages = std::max<size_t>(1, (rows * 100) / 4096);

  double io_cost = static_cast<double>(pages) * SEQ_PAGE_COST;
  double cpu_cost = static_cast<double>(rows) * TUPLE_CPU_COST;

  if (node->predicate()) {
    cpu_cost += static_cast<double>(rows) * TUPLE_CPU_COST;
  }

  return io_cost + cpu_cost;
}

double CostModel::cost_index_scan(const IndexScanPlanNode *node) const {
  size_t total_rows = statistics_->table_cardinality(node->table_oid());

  // Index seek cost: O(log n)
  double seek_cost =
      std::log2(static_cast<double>(std::max<size_t>(1, total_rows))) *
      RANDOM_PAGE_COST;

  size_t matching_rows = 0;
  switch (node->scan_type()) {
  case IndexScanPlanNode::ScanType::POINT_LOOKUP:
    matching_rows = 1;
    break;
  case IndexScanPlanNode::ScanType::RANGE_SCAN:
    matching_rows =
        static_cast<size_t>(static_cast<double>(total_rows) *
                            Statistics::RANGE_SELECTIVITY);
    break;
  case IndexScanPlanNode::ScanType::FULL_SCAN:
    matching_rows = total_rows;
    break;
  }

  // Fetch cost: random I/O for each RID
  double fetch_cost = static_cast<double>(matching_rows) * INDEX_TUPLE_COST;

  return seek_cost + fetch_cost;
}

double CostModel::cost_filter(const FilterPlanNode *node) const {
  size_t input_rows = 0;
  if (!node->children().empty()) {
    input_rows = estimate_cardinality(node->children()[0].get());
  }
  return static_cast<double>(input_rows) * TUPLE_CPU_COST;
}

double CostModel::cost_projection(const ProjectionPlanNode *node) const {
  size_t input_rows = 0;
  if (!node->children().empty()) {
    input_rows = estimate_cardinality(node->children()[0].get());
  }
  return static_cast<double>(input_rows) * TUPLE_CPU_COST * 0.5;
}

double CostModel::cost_sort(const SortPlanNode *node) const {
  size_t input_rows = 0;
  if (!node->children().empty()) {
    input_rows = estimate_cardinality(node->children()[0].get());
  }

  if (input_rows == 0)
    return 0.0;

  // O(n log n) sort cost
  double input_rows_f = static_cast<double>(input_rows);
  double comparisons = input_rows_f * std::log2(input_rows_f);
  return comparisons * SORT_COST;
}

double CostModel::cost_limit(const LimitPlanNode * /*node*/) const {
  // LIMIT is essentially free - just counting
  return 0.0;
}

double
CostModel::cost_nested_loop_join(const NestedLoopJoinPlanNode *node) const {
  if (node->children().size() < 2)
    return 0.0;

  size_t left_rows = estimate_cardinality(node->children()[0].get());
  size_t right_rows = estimate_cardinality(node->children()[1].get());

  // O(n × m) comparisons
  return static_cast<double>(left_rows) * static_cast<double>(right_rows) *
         JOIN_COST;
}

double CostModel::cost_hash_join(const HashJoinPlanNode *node) const {
  if (node->children().size() < 2)
    return 0.0;

  size_t build_rows = estimate_cardinality(node->children()[0].get());
  size_t probe_rows = estimate_cardinality(node->children()[1].get());

  // Build phase: hash all build rows
  double build_cost = static_cast<double>(build_rows) * HASH_BUILD_COST;
  // Probe phase: hash lookup for each probe row
  double probe_cost = static_cast<double>(probe_rows) * HASH_QUAL_COST;

  return build_cost + probe_cost;
}

double CostModel::cost_aggregation(const AggregationPlanNode *node) const {
  size_t input_rows = 0;
  if (!node->children().empty()) {
    input_rows = estimate_cardinality(node->children()[0].get());
  }

  // Hash aggregation: O(n)
  double agg_cost = static_cast<double>(input_rows) * TUPLE_CPU_COST;

  // GROUP BY adds hashing cost
  if (!node->group_by_columns().empty()) {
    agg_cost += static_cast<double>(input_rows) * HASH_QUAL_COST;
  }

  return agg_cost;
}

double CostModel::children_cost(const PlanNode *node) const {
  double total = 0.0;
  for (const auto &child : node->children()) {
    total += estimate_cost(child.get());
  }
  return total;
}

} // namespace entropy
