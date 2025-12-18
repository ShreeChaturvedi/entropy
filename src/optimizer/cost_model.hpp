#pragma once

/**
 * @file cost_model.hpp
 * @brief Cost estimation for query optimization
 *
 * Cost Model estimates the execution cost of query plans:
 * - I/O Cost: Pages read/written
 * - CPU Cost: Per-tuple processing
 *
 * Cost Formulas (production RDBMS style):
 * - SeqScan: pages × SEQ_PAGE_COST
 * - IndexScan: log(rows) × INDEX_COST + matching_rows × TUPLE_COST
 * - HashJoin: build_rows + probe_rows
 * - Sort: rows × log(rows) × SORT_COST
 */

#include <memory>

#include "optimizer/plan_node.hpp"
#include "optimizer/statistics.hpp"

namespace entropy {

/**
 * @brief Cost estimation for query plans
 */
class CostModel {
public:
  explicit CostModel(std::shared_ptr<Statistics> statistics);

  /**
   * @brief Estimate total cost of a plan tree
   */
  [[nodiscard]] double estimate_cost(const PlanNode *plan) const;

  /**
   * @brief Estimate output cardinality of a plan
   */
  [[nodiscard]] size_t estimate_cardinality(const PlanNode *plan) const;

  // ─────────────────────────────────────────────────────────────────────────
  // Cost Factors (tunable for different hardware)
  // ─────────────────────────────────────────────────────────────────────────

  // I/O costs
  static constexpr double SEQ_PAGE_COST = 1.0;    // Sequential page read
  static constexpr double RANDOM_PAGE_COST = 4.0; // Random page read (index)

  // CPU costs
  static constexpr double TUPLE_CPU_COST = 0.01;    // Process one tuple
  static constexpr double INDEX_TUPLE_COST = 0.005; // Fetch via index
  static constexpr double HASH_QUAL_COST = 0.025;   // Hash key comparison
  static constexpr double SORT_COST = 0.02;         // Compare for sorting

  // Operator overhead
  static constexpr double HASH_BUILD_COST = 0.05; // Build hash table entry
  static constexpr double JOIN_COST = 0.01;       // Per-join comparison

private:
  double cost_seq_scan(const SeqScanPlanNode *node) const;
  double cost_index_scan(const IndexScanPlanNode *node) const;
  double cost_filter(const FilterPlanNode *node) const;
  double cost_projection(const ProjectionPlanNode *node) const;
  double cost_sort(const SortPlanNode *node) const;
  double cost_limit(const LimitPlanNode *node) const;
  double cost_nested_loop_join(const NestedLoopJoinPlanNode *node) const;
  double cost_hash_join(const HashJoinPlanNode *node) const;
  double cost_aggregation(const AggregationPlanNode *node) const;

  /**
   * @brief Sum costs of all children
   */
  double children_cost(const PlanNode *node) const;

  std::shared_ptr<Statistics> statistics_;
};

} // namespace entropy
