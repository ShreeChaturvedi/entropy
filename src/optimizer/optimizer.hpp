#pragma once

/**
 * @file optimizer.hpp
 * @brief Cost-based query optimizer
 *
 * The optimizer turns a logical scan/join description into a costed physical
 * plan (a PlanNode tree) that the execution layer instantiates into executors:
 *
 *  - Access-path selection: for each base relation it asks the IndexSelector
 *    whether an index scan beats a sequential scan for that relation's pushed-
 *    down predicate, and emits the cheaper SeqScan / IndexScan node.
 *  - Join-algorithm selection: for each join it costs a hash join (only when
 *    the ON condition is a single equi-predicate) against a nested-loop join
 *    with the CostModel and emits the cheaper node.
 *
 * Relations are joined left-deep in query order; the driving relation is
 * relations[0] and joins[i] joins the accumulated left result with
 * relations[i + 1]. Output column order therefore follows the query, which the
 * projection layer above the join relies on.
 */

#include <memory>
#include <vector>

#include "entropy/status.hpp"
#include "optimizer/cost_model.hpp"
#include "optimizer/index_selector.hpp"
#include "optimizer/plan_node.hpp"
#include "optimizer/statistics.hpp"
#include "parser/expression.hpp"

namespace entropy {

class Catalog;
class Schema;

/**
 * @brief One base relation participating in a scan/join plan.
 *
 * @c schema and any resolved column indices in @c filter refer to that base
 * table; @c filter (may be null) is the single-table predicate pushed down to
 * the scan. Ownership of @c filter transfers to the emitted plan node.
 */
struct ScanSpec {
  oid_t table_oid = INVALID_OID;
  const Schema *schema = nullptr;
  std::unique_ptr<Expression> filter;
};

/**
 * @brief One join in the left-deep chain.
 *
 * @c condition is the full join predicate, already resolved against the
 * combined (left ++ right) schema, and is evaluated by a nested-loop join.
 * When @c is_equi is set the join is a single `left_col = right_col` equality,
 * so a hash join is a candidate; @c left_key_index indexes the combined left
 * schema and @c right_key_index indexes the right relation's schema.
 */
struct JoinSpec {
  NestedLoopJoinPlanNode::JoinType join_type =
      NestedLoopJoinPlanNode::JoinType::INNER;
  std::unique_ptr<Expression> condition;
  bool is_equi = false;
  size_t left_key_index = 0;
  size_t right_key_index = 0;
  // Combined output schema after this join (owned by the caller).
  const Schema *output_schema = nullptr;
};

class Optimizer {
public:
  Optimizer(std::shared_ptr<Catalog> catalog,
            std::shared_ptr<Statistics> statistics,
            std::shared_ptr<CostModel> cost_model,
            const IndexSelector *index_selector);

  /**
   * @brief Build a costed physical scan/join plan.
   *
   * @param relations Base relations, driving relation first (non-empty).
   * @param joins     Joins; joins[i] joins relations[0..i] with relations[i+1],
   *                  so joins.size() + 1 == relations.size().
   * @param plan      Output physical plan root.
   */
  [[nodiscard]] Status optimize(std::vector<ScanSpec> relations,
                                std::vector<JoinSpec> joins,
                                std::unique_ptr<PlanNode> *plan);

private:
  // Emit the cheaper of a sequential and an index scan for one relation.
  [[nodiscard]] std::unique_ptr<PlanNode> plan_scan(ScanSpec spec) const;

  std::shared_ptr<Catalog> catalog_;
  std::shared_ptr<Statistics> statistics_;
  std::shared_ptr<CostModel> cost_model_;
  const IndexSelector *index_selector_;
};

} // namespace entropy
