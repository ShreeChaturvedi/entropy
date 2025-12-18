#pragma once

/**
 * @file statement.hpp
 * @brief SQL Statement AST nodes
 *
 * Defines the Abstract Syntax Tree nodes for parsed SQL statements.
 * Each statement type has its own class with specific fields.
 */

#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "common/types.hpp"
#include "parser/expression.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Statement Types
// ─────────────────────────────────────────────────────────────────────────────

enum class StatementType {
  SELECT,
  INSERT,
  UPDATE,
  DELETE_STMT, // Avoid conflict with DELETE macro
  CREATE_TABLE,
  DROP_TABLE,
  CREATE_INDEX,
  DROP_INDEX,
  EXPLAIN,
};

// Forward declarations
class Expression;

// ─────────────────────────────────────────────────────────────────────────────
// Base Statement Class
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Base class for all SQL statements
 */
class Statement {
public:
  explicit Statement(StatementType type) : type_(type) {}
  virtual ~Statement() = default;

  [[nodiscard]] StatementType type() const noexcept { return type_; }

private:
  StatementType type_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Column Definition (for CREATE TABLE)
// ─────────────────────────────────────────────────────────────────────────────

struct ColumnDef {
  std::string name;
  TypeId type;
  size_t length = 0; // For VARCHAR
  bool nullable = true;
  bool is_primary_key = false;
};

// ─────────────────────────────────────────────────────────────────────────────
// SELECT Statement
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Represents a SELECT column (can be * or specific columns)
 */
struct SelectColumn {
  bool is_star = false;                   // SELECT *
  std::string column_name;                // Column name
  std::string table_alias;                // Optional table alias
  std::string alias;                      // AS alias
  std::unique_ptr<Expression> expression; // Computed column

  SelectColumn() = default;

  static SelectColumn star() {
    SelectColumn col;
    col.is_star = true;
    return col;
  }

  static SelectColumn column(std::string name) {
    SelectColumn col;
    col.column_name = std::move(name);
    return col;
  }
};

/**
 * @brief Table reference in FROM clause
 */
struct TableRef {
  std::string table_name;
  std::string alias;
};

/**
 * @brief ORDER BY clause item
 */
struct OrderByItem {
  std::string column_name;
  bool ascending = true;
};

/**
 * @brief Type of JOIN operation
 */
enum class JoinType {
  INNER, // INNER JOIN (default)
  LEFT,  // LEFT [OUTER] JOIN
  RIGHT, // RIGHT [OUTER] JOIN
  CROSS, // CROSS JOIN (no condition)
};

/**
 * @brief JOIN clause: table JOIN other_table ON condition
 */
struct JoinClause {
  TableRef table;                        // The table being joined
  JoinType type = JoinType::INNER;       // JOIN type
  std::unique_ptr<Expression> condition; // ON condition (null for CROSS JOIN)
};

/**
 * @brief SELECT statement with optional JOIN support
 * SELECT columns FROM table [JOIN table ON cond]* [WHERE expr] [ORDER BY]
 * [LIMIT n]
 */
class SelectStatement : public Statement {
public:
  SelectStatement() : Statement(StatementType::SELECT) {}

  std::vector<SelectColumn> columns;
  TableRef table;                // Primary table in FROM
  std::vector<JoinClause> joins; // Optional JOINs
  std::unique_ptr<Expression> where_clause;
  std::vector<OrderByItem> order_by;
  std::optional<size_t> limit;
  std::optional<size_t> offset;
};

// ─────────────────────────────────────────────────────────────────────────────
// INSERT Statement
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief A single value in INSERT (literal or expression)
 */
using InsertValue =
    std::variant<int64_t, double, std::string, bool, std::monostate>;

/**
 * @brief INSERT statement: INSERT INTO table (cols) VALUES (vals), (vals), ...
 */
class InsertStatement : public Statement {
public:
  InsertStatement() : Statement(StatementType::INSERT) {}

  std::string table_name;
  std::vector<std::string> columns;             // Optional column list
  std::vector<std::vector<InsertValue>> values; // Rows of values
};

// ─────────────────────────────────────────────────────────────────────────────
// UPDATE Statement
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief SET clause item: column = value
 */
struct SetClause {
  std::string column_name;
  std::unique_ptr<Expression> value;
};

/**
 * @brief UPDATE statement: UPDATE table SET col=val, ... WHERE expr
 */
class UpdateStatement : public Statement {
public:
  UpdateStatement() : Statement(StatementType::UPDATE) {}

  std::string table_name;
  std::vector<SetClause> set_clauses;
  std::unique_ptr<Expression> where_clause;
};

// ─────────────────────────────────────────────────────────────────────────────
// DELETE Statement
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief DELETE statement: DELETE FROM table WHERE expr
 */
class DeleteStatement : public Statement {
public:
  DeleteStatement() : Statement(StatementType::DELETE_STMT) {}

  std::string table_name;
  std::unique_ptr<Expression> where_clause;
};

// ─────────────────────────────────────────────────────────────────────────────
// CREATE TABLE Statement
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief CREATE TABLE statement: CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
 */
class CreateTableStatement : public Statement {
public:
  CreateTableStatement() : Statement(StatementType::CREATE_TABLE) {}

  std::string table_name;
  std::vector<ColumnDef> columns;
  bool if_not_exists = false;
};

// ─────────────────────────────────────────────────────────────────────────────
// DROP TABLE Statement
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief DROP TABLE statement: DROP TABLE name
 */
class DropTableStatement : public Statement {
public:
  DropTableStatement() : Statement(StatementType::DROP_TABLE) {}

  std::string table_name;
  bool if_exists = false;
};

// ─────────────────────────────────────────────────────────────────────────────
// EXPLAIN Statement
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief EXPLAIN statement: EXPLAIN SELECT ...
 *
 * Wraps another statement and returns execution plan instead of results.
 */
class ExplainStatement : public Statement {
public:
  ExplainStatement() : Statement(StatementType::EXPLAIN) {}

  std::unique_ptr<Statement> inner_statement;
  bool analyze = false; // EXPLAIN ANALYZE for actual execution stats
};

} // namespace entropy
