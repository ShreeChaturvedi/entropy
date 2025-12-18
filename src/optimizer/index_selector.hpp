#pragma once

/**
 * @file index_selector.hpp
 * @brief Automatic index selection for query optimization
 *
 * IndexSelector decides when to use IndexScan vs SeqScan:
 * 1. Check if column has an index (via Catalog)
 * 2. Estimate costs of both approaches
 * 3. Choose lower cost option
 *
 * Heuristics:
 * - Point lookup (=): Always prefer index if available
 * - Range scan (<, >, BETWEEN): Prefer index if selectivity < 30%
 * - Full scan: Prefer SeqScan (better sequential I/O)
 */

#include <memory>
#include <optional>

#include "common/types.hpp"
#include "optimizer/cost_model.hpp"
#include "optimizer/plan_node.hpp"
#include "optimizer/statistics.hpp"

namespace entropy {

class Catalog;

/**
 * @brief Result of index selection
 */
struct IndexSelection {
  bool use_index = false;
  oid_t index_oid = 0;
  IndexScanPlanNode::ScanType scan_type =
      IndexScanPlanNode::ScanType::FULL_SCAN;
  std::optional<BPTreeKey> start_key;
  std::optional<BPTreeKey> end_key;
  double index_cost = 0.0;
  double seq_scan_cost = 0.0;
};

/**
 * @brief Automatic index selection
 */
class IndexSelector {
public:
  IndexSelector(std::shared_ptr<Catalog> catalog,
                std::shared_ptr<Statistics> statistics,
                std::shared_ptr<CostModel> cost_model);

  /**
   * @brief Select best access method for a table scan
   * @param table_oid Table to scan
   * @param predicate Optional predicate (WHERE clause)
   * @return Index selection result
   */
  [[nodiscard]] IndexSelection
  select_access_method(oid_t table_oid, const Expression *predicate) const;

  /**
   * @brief Check if an index exists on a column
   */
  [[nodiscard]] std::optional<oid_t> find_index(oid_t table_oid,
                                                column_id_t column_id) const;

private:
  /**
   * @brief Extract column and constant from equality predicate
   */
  bool extract_point_lookup(const Expression *predicate, column_id_t *column,
                            BPTreeKey *key) const;

  /**
   * @brief Extract range bounds from comparison predicate
   */
  bool extract_range_scan(const Expression *predicate, column_id_t *column,
                          std::optional<BPTreeKey> *start,
                          std::optional<BPTreeKey> *end) const;

  std::shared_ptr<Catalog> catalog_;
  std::shared_ptr<Statistics> statistics_;
  std::shared_ptr<CostModel> cost_model_;
};

} // namespace entropy
