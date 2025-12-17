#pragma once

/**
 * @file binder.hpp
 * @brief Semantic analysis - name resolution and type checking
 *
 * The Binder performs semantic analysis on parsed SQL statements:
 * - Resolves table names to catalog entries
 * - Resolves column names to schema positions
 * - Performs type checking and validation
 * - Sets column indices on ColumnRefExpressions
 */

#include <memory>
#include <string>
#include <vector>

#include "catalog/catalog.hpp"
#include "entropy/status.hpp"
#include "parser/expression.hpp"
#include "parser/statement.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Bound Statement Types
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Context for a bound SELECT statement
 */
struct BoundSelectContext {
  TableInfo *table_info = nullptr;       ///< Resolved table
  std::vector<size_t> column_indices;    ///< Indices of selected columns
  bool select_all = false;               ///< SELECT *
  std::unique_ptr<Expression> predicate; ///< Bound WHERE clause
};

/**
 * @brief Context for a bound INSERT statement
 */
struct BoundInsertContext {
  TableInfo *table_info = nullptr;    ///< Resolved table
  std::vector<size_t> column_indices; ///< Indices of columns to insert
};

/**
 * @brief Context for a bound UPDATE statement
 */
struct BoundUpdateContext {
  TableInfo *table_info = nullptr;    ///< Resolved table
  std::vector<size_t> column_indices; ///< Indices of columns to update
  std::vector<std::unique_ptr<Expression>> values; ///< Values to set
  std::unique_ptr<Expression> predicate;           ///< Bound WHERE clause
};

/**
 * @brief Context for a bound DELETE statement
 */
struct BoundDeleteContext {
  TableInfo *table_info = nullptr;       ///< Resolved table
  std::unique_ptr<Expression> predicate; ///< Bound WHERE clause
};

// ─────────────────────────────────────────────────────────────────────────────
// Binder
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Semantic analyzer for SQL statements
 *
 * Resolves names and performs type checking:
 * - Table names are resolved against the catalog
 * - Column names are resolved to schema positions
 * - Expression types are validated
 */
class Binder {
public:
  /**
   * @brief Construct a binder with catalog access
   * @param catalog Catalog for name resolution
   */
  explicit Binder(Catalog *catalog);

  // ─────────────────────────────────────────────────────────────────────────
  // Statement Binding
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Bind a SELECT statement
   * @param stmt Parsed SELECT statement
   * @param context Output bound context
   * @return Status::Ok() on success
   */
  [[nodiscard]] Status bind_select(SelectStatement *stmt,
                                   BoundSelectContext *context);

  /**
   * @brief Bind an INSERT statement
   * @param stmt Parsed INSERT statement
   * @param context Output bound context
   * @return Status::Ok() on success
   */
  [[nodiscard]] Status bind_insert(InsertStatement *stmt,
                                   BoundInsertContext *context);

  /**
   * @brief Bind an UPDATE statement
   * @param stmt Parsed UPDATE statement
   * @param context Output bound context
   * @return Status::Ok() on success
   */
  [[nodiscard]] Status bind_update(UpdateStatement *stmt,
                                   BoundUpdateContext *context);

  /**
   * @brief Bind a DELETE statement
   * @param stmt Parsed DELETE statement
   * @param context Output bound context
   * @return Status::Ok() on success
   */
  [[nodiscard]] Status bind_delete(DeleteStatement *stmt,
                                   BoundDeleteContext *context);

  // ─────────────────────────────────────────────────────────────────────────
  // Expression Binding
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Bind an expression in the context of a table
   * @param expr Expression to bind
   * @param table_info Table context for column resolution
   * @return Status::Ok() on success
   */
  [[nodiscard]] Status bind_expression(Expression *expr,
                                       const TableInfo *table_info);

private:
  /**
   * @brief Resolve a table name to TableInfo
   */
  [[nodiscard]] Status resolve_table(const std::string &table_name,
                                     TableInfo **out);

  /**
   * @brief Resolve a column name to its index in the schema
   */
  [[nodiscard]] Status resolve_column(const std::string &column_name,
                                      const Schema &schema, size_t *out_index,
                                      TypeId *out_type);

  Catalog *catalog_;
};

} // namespace entropy
