#pragma once

/**
 * @file plan_node.hpp
 * @brief Query plan nodes for the optimizer
 *
 * Design Goals:
 * 1. Type-safe plan node hierarchy
 * 2. Each node carries output schema and cost estimate
 * 3. Support for both logical and physical plans
 * 4. Clean separation between plan types
 *
 * Plan Node Types:
 * - Scan: SeqScan, IndexScan
 * - Modify: Insert, Update, Delete
 * - Transform: Filter, Projection, Sort, Limit
 * - Join: NestedLoopJoin, HashJoin
 * - Aggregate: Aggregation with GROUP BY
 */

#include <memory>
#include <optional>
#include <vector>

#include "catalog/schema.hpp"
#include "common/types.hpp"
#include "parser/expression.hpp"
#include "storage/b_plus_tree.hpp"

namespace entropy {

// Forward declarations
class Schema;

// ─────────────────────────────────────────────────────────────────────────────
// Plan Node Types
// ─────────────────────────────────────────────────────────────────────────────

enum class PlanNodeType {
  SEQ_SCAN,
  INDEX_SCAN,
  INSERT,
  UPDATE,
  DELETE,
  NESTED_LOOP_JOIN,
  HASH_JOIN,
  AGGREGATION,
  PROJECTION,
  FILTER,
  SORT,
  LIMIT,
};

// ─────────────────────────────────────────────────────────────────────────────
// Base PlanNode
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Base class for all plan nodes
 */
class PlanNode {
public:
  explicit PlanNode(PlanNodeType type) : type_(type) {}
  virtual ~PlanNode() = default;

  [[nodiscard]] PlanNodeType type() const noexcept { return type_; }
  [[nodiscard]] const std::vector<std::unique_ptr<PlanNode>> &children() const {
    return children_;
  }

  void add_child(std::unique_ptr<PlanNode> child) {
    children_.push_back(std::move(child));
  }

  /**
   * @brief Get estimated cost of this plan
   */
  [[nodiscard]] double cost() const noexcept { return cost_; }
  void set_cost(double cost) { cost_ = cost; }

  /**
   * @brief Get output schema for this plan node
   */
  [[nodiscard]] virtual const Schema *output_schema() const = 0;

private:
  PlanNodeType type_;
  std::vector<std::unique_ptr<PlanNode>> children_;
  double cost_ = 0.0;
};

// ─────────────────────────────────────────────────────────────────────────────
// Scan Nodes
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Sequential scan of a table
 */
class SeqScanPlanNode : public PlanNode {
public:
  SeqScanPlanNode(oid_t table_oid, const Schema *output_schema,
                  std::unique_ptr<Expression> predicate = nullptr)
      : PlanNode(PlanNodeType::SEQ_SCAN), table_oid_(table_oid),
        output_schema_(output_schema), predicate_(std::move(predicate)) {}

  [[nodiscard]] const Schema *output_schema() const override {
    return output_schema_;
  }
  [[nodiscard]] oid_t table_oid() const { return table_oid_; }
  [[nodiscard]] const Expression *predicate() const { return predicate_.get(); }

private:
  oid_t table_oid_;
  const Schema *output_schema_;
  std::unique_ptr<Expression> predicate_;
};

/**
 * @brief Index scan using B+ tree
 */
class IndexScanPlanNode : public PlanNode {
public:
  enum class ScanType { POINT_LOOKUP, RANGE_SCAN, FULL_SCAN };

  IndexScanPlanNode(oid_t table_oid, oid_t index_oid,
                    const Schema *output_schema, ScanType scan_type,
                    std::optional<BPTreeKey> start_key = std::nullopt,
                    std::optional<BPTreeKey> end_key = std::nullopt)
      : PlanNode(PlanNodeType::INDEX_SCAN), table_oid_(table_oid),
        index_oid_(index_oid), output_schema_(output_schema),
        scan_type_(scan_type), start_key_(start_key), end_key_(end_key) {}

  [[nodiscard]] const Schema *output_schema() const override {
    return output_schema_;
  }
  [[nodiscard]] oid_t table_oid() const { return table_oid_; }
  [[nodiscard]] oid_t index_oid() const { return index_oid_; }
  [[nodiscard]] ScanType scan_type() const { return scan_type_; }
  [[nodiscard]] std::optional<BPTreeKey> start_key() const {
    return start_key_;
  }
  [[nodiscard]] std::optional<BPTreeKey> end_key() const { return end_key_; }

private:
  oid_t table_oid_;
  oid_t index_oid_;
  const Schema *output_schema_;
  ScanType scan_type_;
  std::optional<BPTreeKey> start_key_;
  std::optional<BPTreeKey> end_key_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Transform Nodes
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Filter (WHERE clause)
 */
class FilterPlanNode : public PlanNode {
public:
  FilterPlanNode(std::unique_ptr<Expression> predicate,
                 const Schema *output_schema)
      : PlanNode(PlanNodeType::FILTER), predicate_(std::move(predicate)),
        output_schema_(output_schema) {}

  [[nodiscard]] const Schema *output_schema() const override {
    return output_schema_;
  }
  [[nodiscard]] const Expression *predicate() const { return predicate_.get(); }

private:
  std::unique_ptr<Expression> predicate_;
  const Schema *output_schema_;
};

/**
 * @brief Projection (SELECT columns)
 */
class ProjectionPlanNode : public PlanNode {
public:
  ProjectionPlanNode(std::vector<size_t> column_indices,
                     const Schema *output_schema)
      : PlanNode(PlanNodeType::PROJECTION),
        column_indices_(std::move(column_indices)),
        output_schema_(output_schema) {}

  [[nodiscard]] const Schema *output_schema() const override {
    return output_schema_;
  }
  [[nodiscard]] const std::vector<size_t> &column_indices() const {
    return column_indices_;
  }

private:
  std::vector<size_t> column_indices_;
  const Schema *output_schema_;
};

/**
 * @brief Sort (ORDER BY)
 */
class SortPlanNode : public PlanNode {
public:
  struct SortKey {
    size_t column_index;
    bool ascending;
  };

  SortPlanNode(std::vector<SortKey> sort_keys, const Schema *output_schema)
      : PlanNode(PlanNodeType::SORT), sort_keys_(std::move(sort_keys)),
        output_schema_(output_schema) {}

  [[nodiscard]] const Schema *output_schema() const override {
    return output_schema_;
  }
  [[nodiscard]] const std::vector<SortKey> &sort_keys() const {
    return sort_keys_;
  }

private:
  std::vector<SortKey> sort_keys_;
  const Schema *output_schema_;
};

/**
 * @brief Limit (LIMIT/OFFSET)
 */
class LimitPlanNode : public PlanNode {
public:
  LimitPlanNode(std::optional<size_t> limit, size_t offset,
                const Schema *output_schema)
      : PlanNode(PlanNodeType::LIMIT), limit_(limit), offset_(offset),
        output_schema_(output_schema) {}

  [[nodiscard]] const Schema *output_schema() const override {
    return output_schema_;
  }
  [[nodiscard]] std::optional<size_t> limit() const { return limit_; }
  [[nodiscard]] size_t offset() const { return offset_; }

private:
  std::optional<size_t> limit_;
  size_t offset_;
  const Schema *output_schema_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Join Nodes
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Nested loop join
 */
class NestedLoopJoinPlanNode : public PlanNode {
public:
  enum class JoinType { INNER, LEFT, RIGHT, CROSS };

  NestedLoopJoinPlanNode(JoinType join_type,
                         std::unique_ptr<Expression> condition,
                         const Schema *output_schema)
      : PlanNode(PlanNodeType::NESTED_LOOP_JOIN), join_type_(join_type),
        condition_(std::move(condition)), output_schema_(output_schema) {}

  [[nodiscard]] const Schema *output_schema() const override {
    return output_schema_;
  }
  [[nodiscard]] JoinType join_type() const { return join_type_; }
  [[nodiscard]] const Expression *condition() const { return condition_.get(); }

private:
  JoinType join_type_;
  std::unique_ptr<Expression> condition_;
  const Schema *output_schema_;
};

/**
 * @brief Hash join for equi-joins
 */
class HashJoinPlanNode : public PlanNode {
public:
  enum class JoinType { INNER, LEFT, RIGHT };

  HashJoinPlanNode(JoinType join_type, size_t left_key_index,
                   size_t right_key_index, const Schema *output_schema)
      : PlanNode(PlanNodeType::HASH_JOIN), join_type_(join_type),
        left_key_index_(left_key_index), right_key_index_(right_key_index),
        output_schema_(output_schema) {}

  [[nodiscard]] const Schema *output_schema() const override {
    return output_schema_;
  }
  [[nodiscard]] JoinType join_type() const { return join_type_; }
  [[nodiscard]] size_t left_key_index() const { return left_key_index_; }
  [[nodiscard]] size_t right_key_index() const { return right_key_index_; }

private:
  JoinType join_type_;
  size_t left_key_index_;
  size_t right_key_index_;
  const Schema *output_schema_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation Node
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Aggregation with optional GROUP BY
 */
class AggregationPlanNode : public PlanNode {
public:
  enum class AggType { COUNT, COUNT_STAR, SUM, AVG, MIN, MAX };

  struct AggExpr {
    AggType type;
    std::optional<size_t> column_index; // nullopt for COUNT(*)
  };

  AggregationPlanNode(std::vector<size_t> group_by_columns,
                      std::vector<AggExpr> aggregates,
                      const Schema *output_schema)
      : PlanNode(PlanNodeType::AGGREGATION),
        group_by_columns_(std::move(group_by_columns)),
        aggregates_(std::move(aggregates)), output_schema_(output_schema) {}

  [[nodiscard]] const Schema *output_schema() const override {
    return output_schema_;
  }
  [[nodiscard]] const std::vector<size_t> &group_by_columns() const {
    return group_by_columns_;
  }
  [[nodiscard]] const std::vector<AggExpr> &aggregates() const {
    return aggregates_;
  }

private:
  std::vector<size_t> group_by_columns_;
  std::vector<AggExpr> aggregates_;
  const Schema *output_schema_;
};

} // namespace entropy
