/**
 * @file optimizer.cpp
 * @brief Cost-based optimizer implementation
 */

#include "optimizer/optimizer.hpp"

#include <utility>

#include "catalog/schema.hpp"

namespace entropy {

Optimizer::Optimizer(std::shared_ptr<Catalog> catalog,
                     std::shared_ptr<Statistics> statistics,
                     std::shared_ptr<CostModel> cost_model,
                     const IndexSelector *index_selector)
    : catalog_(std::move(catalog)), statistics_(std::move(statistics)),
      cost_model_(std::move(cost_model)), index_selector_(index_selector) {}

std::unique_ptr<PlanNode> Optimizer::plan_scan(ScanSpec spec) const {
  // Access-path selection: let the index selector cost an index scan against a
  // sequential scan for this relation's pushed-down predicate.
  IndexSelection sel =
      index_selector_->select_access_method(spec.table_oid, spec.filter.get());

  std::unique_ptr<PlanNode> node;
  if (sel.use_index) {
    // The index fully covers the predicate the selector recognised (a single
    // point/range on the indexed column), so no residual filter is attached.
    node = std::make_unique<IndexScanPlanNode>(
        spec.table_oid, sel.index_oid, spec.schema, sel.scan_type,
        sel.start_key, sel.end_key);
  } else {
    node = std::make_unique<SeqScanPlanNode>(spec.table_oid, spec.schema,
                                             std::move(spec.filter));
  }
  node->set_cost(cost_model_->estimate_cost(node.get()));
  return node;
}

Status Optimizer::optimize(std::vector<ScanSpec> relations,
                           std::vector<JoinSpec> joins,
                           std::unique_ptr<PlanNode> *plan) {
  if (relations.empty()) {
    return Status::InvalidArgument("optimizer: no relations to plan");
  }
  if (joins.size() + 1 != relations.size()) {
    return Status::InvalidArgument(
        "optimizer: join count does not match relation count");
  }

  std::unique_ptr<PlanNode> root = plan_scan(std::move(relations[0]));

  for (size_t i = 0; i < joins.size(); ++i) {
    JoinSpec &spec = joins[i];
    std::unique_ptr<PlanNode> right = plan_scan(std::move(relations[i + 1]));

    const double left_cost = root->cost();
    const double right_cost = right->cost();
    const auto left_rows =
        static_cast<double>(cost_model_->estimate_cardinality(root.get()));
    const auto right_rows =
        static_cast<double>(cost_model_->estimate_cardinality(right.get()));

    // A hash join is only legal for a single equi-predicate and the inner/
    // outer join kinds the hash operator implements (not CROSS).
    const bool hash_eligible =
        spec.is_equi &&
        spec.join_type != NestedLoopJoinPlanNode::JoinType::CROSS;

    const double nlj_cost =
        left_cost + right_cost + left_rows * right_rows * CostModel::JOIN_COST;
    const double hash_cost = left_cost + right_cost +
                             left_rows * CostModel::HASH_BUILD_COST +
                             right_rows * CostModel::HASH_QUAL_COST;

    std::unique_ptr<PlanNode> join_node;
    if (hash_eligible && hash_cost <= nlj_cost) {
      HashJoinPlanNode::JoinType ht = HashJoinPlanNode::JoinType::INNER;
      switch (spec.join_type) {
      case NestedLoopJoinPlanNode::JoinType::LEFT:
        ht = HashJoinPlanNode::JoinType::LEFT;
        break;
      case NestedLoopJoinPlanNode::JoinType::RIGHT:
        ht = HashJoinPlanNode::JoinType::RIGHT;
        break;
      case NestedLoopJoinPlanNode::JoinType::INNER:
      case NestedLoopJoinPlanNode::JoinType::CROSS:
        ht = HashJoinPlanNode::JoinType::INNER;
        break;
      }
      join_node = std::make_unique<HashJoinPlanNode>(
          ht, spec.left_key_index, spec.right_key_index, spec.output_schema);
    } else {
      join_node = std::make_unique<NestedLoopJoinPlanNode>(
          spec.join_type, std::move(spec.condition), spec.output_schema);
    }

    join_node->add_child(std::move(root));
    join_node->add_child(std::move(right));
    join_node->set_cost(cost_model_->estimate_cost(join_node.get()));
    root = std::move(join_node);
  }

  *plan = std::move(root);
  return Status::Ok();
}

} // namespace entropy
