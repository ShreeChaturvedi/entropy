#pragma once

/**
 * @file statistics.hpp
 * @brief Table and column statistics for cost-based optimization
 *
 * Statistics enable the optimizer to estimate:
 * 1. Cardinality - number of rows after each operator
 * 2. Selectivity - fraction of rows passing predicates
 * 3. Cost - I/O and CPU cost of plan alternatives
 *
 * Key Statistics:
 * - Table: row_count, page_count
 * - Column: distinct_values, null_fraction, min/max for range predicates
 */

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "common/types.hpp"
#include "parser/expression.hpp"
#include "storage/tuple.hpp"

namespace entropy {

// Forward declarations
class Catalog;
class TableHeap;
class Schema;

// ─────────────────────────────────────────────────────────────────────────────
// Column Statistics
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Statistics for a single column
 */
struct ColumnStatistics {
  double distinct_values = 0;          // Number of distinct values
  double null_fraction = 0.0;          // Fraction of NULL values (0.0-1.0)
  std::optional<TupleValue> min_value; // Minimum value (for range predicates)
  std::optional<TupleValue> max_value; // Maximum value (for range predicates)
};

// ─────────────────────────────────────────────────────────────────────────────
// Table Statistics
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Statistics for a table
 */
struct TableStatistics {
  size_t row_count = 0;  // Total number of rows
  size_t page_count = 0; // Number of pages
  std::unordered_map<column_id_t, ColumnStatistics> columns;
};

// ─────────────────────────────────────────────────────────────────────────────
// Statistics Manager
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Manages statistics collection and estimation
 */
class Statistics {
public:
  explicit Statistics(std::shared_ptr<Catalog> catalog);

  /**
   * @brief Get table cardinality (row count)
   */
  [[nodiscard]] size_t table_cardinality(oid_t table_oid) const;

  /**
   * @brief Estimate selectivity for a predicate on a column
   *
   * Selectivity = fraction of rows that satisfy the predicate (0.0-1.0)
   * - Equality: 1 / distinct_values
   * - Range: (upper - lower) / (max - min)
   * - Default: 0.1 (10% if unknown)
   */
  [[nodiscard]] double column_selectivity(oid_t table_oid,
                                          column_id_t column_id) const;

  /**
   * @brief Estimate selectivity for an expression predicate
   */
  [[nodiscard]] double estimate_selectivity(oid_t table_oid,
                                            const Expression *predicate) const;

  /**
   * @brief Collect statistics for a table (scan and compute)
   */
  void collect_statistics(oid_t table_oid);

  /**
   * @brief Initialize stats for a newly created table
   */
  void on_table_created(oid_t table_oid);

  /**
   * @brief Drop cached stats for a table
   */
  void on_table_dropped(oid_t table_oid);

  /**
   * @brief Update stats after inserts
   */
  void on_rows_inserted(oid_t table_oid, size_t rows);

  /**
   * @brief Update stats after deletes
   */
  void on_rows_deleted(oid_t table_oid, size_t rows);

  /**
   * @brief Get cached table statistics
   */
  [[nodiscard]] const TableStatistics *get_table_stats(oid_t table_oid) const;

  // Default selectivity values
  static constexpr double DEFAULT_SELECTIVITY = 0.1;
  static constexpr double EQUALITY_SELECTIVITY = 0.01;
  static constexpr double RANGE_SELECTIVITY = 0.33;

private:
  std::shared_ptr<Catalog> catalog_;
  std::unordered_map<oid_t, TableStatistics> table_stats_;
};

} // namespace entropy
