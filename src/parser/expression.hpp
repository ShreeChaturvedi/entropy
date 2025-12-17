#pragma once

/**
 * @file expression.hpp
 * @brief SQL Expression AST and evaluation
 *
 * Expressions are used in:
 * - WHERE clauses (predicates)
 * - SELECT columns (computed values)
 * - SET clauses in UPDATE
 */

#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "common/types.hpp"
#include "storage/tuple.hpp"

namespace entropy {

// Forward declarations
class Schema;
class Tuple;

// ─────────────────────────────────────────────────────────────────────────────
// Expression Types
// ─────────────────────────────────────────────────────────────────────────────

enum class ExpressionType {
  CONSTANT,   // Literal value (1, 'hello', true)
  COLUMN_REF, // Column reference (name, table.name)
  BINARY_OP,  // Binary operator (+, -, *, /)
  COMPARISON, // Comparison (=, <>, <, >, <=, >=)
  LOGICAL,    // Logical (AND, OR, NOT)
  IS_NULL,    // IS NULL / IS NOT NULL
  UNARY,      // Unary operator (-, NOT)
};

// ─────────────────────────────────────────────────────────────────────────────
// Operators
// ─────────────────────────────────────────────────────────────────────────────

enum class BinaryOpType {
  ADD,      // +
  SUBTRACT, // -
  MULTIPLY, // *
  DIVIDE,   // /
};

enum class ComparisonType {
  EQUAL,         // =
  NOT_EQUAL,     // <> or !=
  LESS_THAN,     // <
  LESS_EQUAL,    // <=
  GREATER_THAN,  // >
  GREATER_EQUAL, // >=
};

enum class LogicalOpType {
  AND,
  OR,
  NOT,
};

// ─────────────────────────────────────────────────────────────────────────────
// Expression Base Class
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Base class for all expressions
 */
class Expression {
public:
  explicit Expression(ExpressionType type) : expr_type_(type) {}
  virtual ~Expression() = default;

  [[nodiscard]] ExpressionType expr_type() const noexcept { return expr_type_; }

  /**
   * @brief Evaluate this expression against a tuple
   * @param tuple The tuple to evaluate against
   * @param schema The schema of the tuple
   * @return The result of evaluation
   */
  [[nodiscard]] virtual TupleValue evaluate(const Tuple &tuple,
                                            const Schema &schema) const = 0;

  /**
   * @brief Get the result type of this expression
   */
  [[nodiscard]] virtual TypeId result_type() const = 0;

  /**
   * @brief Create a deep copy of this expression
   */
  [[nodiscard]] virtual std::unique_ptr<Expression> clone() const = 0;

private:
  ExpressionType expr_type_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Constant Expression
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief A constant/literal value expression
 */
class ConstantExpression : public Expression {
public:
  explicit ConstantExpression(TupleValue value);

  [[nodiscard]] TupleValue evaluate(const Tuple &tuple,
                                    const Schema &schema) const override;
  [[nodiscard]] TypeId result_type() const override;
  [[nodiscard]] std::unique_ptr<Expression> clone() const override;

  [[nodiscard]] const TupleValue &value() const noexcept { return value_; }

private:
  TupleValue value_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Column Reference Expression
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Reference to a column in a table
 */
class ColumnRefExpression : public Expression {
public:
  ColumnRefExpression(std::string column_name, std::string table_name = "");

  [[nodiscard]] TupleValue evaluate(const Tuple &tuple,
                                    const Schema &schema) const override;
  [[nodiscard]] TypeId result_type() const override;
  [[nodiscard]] std::unique_ptr<Expression> clone() const override;

  [[nodiscard]] const std::string &column_name() const noexcept {
    return column_name_;
  }
  [[nodiscard]] const std::string &table_name() const noexcept {
    return table_name_;
  }

  // Set after binding
  void set_column_index(size_t idx) { column_idx_ = idx; }
  void set_type(TypeId type) { type_ = type; }
  [[nodiscard]] size_t column_index() const noexcept { return column_idx_; }

private:
  std::string column_name_;
  std::string table_name_;
  size_t column_idx_ = 0;         // Set by binder
  TypeId type_ = TypeId::INVALID; // Set by binder
};

// ─────────────────────────────────────────────────────────────────────────────
// Binary Operation Expression
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Binary arithmetic operation: left OP right
 */
class BinaryOpExpression : public Expression {
public:
  BinaryOpExpression(BinaryOpType op, std::unique_ptr<Expression> left,
                     std::unique_ptr<Expression> right);

  [[nodiscard]] TupleValue evaluate(const Tuple &tuple,
                                    const Schema &schema) const override;
  [[nodiscard]] TypeId result_type() const override;
  [[nodiscard]] std::unique_ptr<Expression> clone() const override;

  [[nodiscard]] BinaryOpType op() const noexcept { return op_; }
  [[nodiscard]] const Expression *left() const noexcept { return left_.get(); }
  [[nodiscard]] const Expression *right() const noexcept {
    return right_.get();
  }

private:
  BinaryOpType op_;
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Comparison Expression
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Comparison operation: left CMP right
 */
class ComparisonExpression : public Expression {
public:
  ComparisonExpression(ComparisonType cmp, std::unique_ptr<Expression> left,
                       std::unique_ptr<Expression> right);

  [[nodiscard]] TupleValue evaluate(const Tuple &tuple,
                                    const Schema &schema) const override;
  [[nodiscard]] TypeId result_type() const override;
  [[nodiscard]] std::unique_ptr<Expression> clone() const override;

  [[nodiscard]] ComparisonType cmp() const noexcept { return cmp_; }
  [[nodiscard]] const Expression *left() const noexcept { return left_.get(); }
  [[nodiscard]] const Expression *right() const noexcept {
    return right_.get();
  }

private:
  ComparisonType cmp_;
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Logical Expression
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Logical operation: AND, OR, NOT
 */
class LogicalExpression : public Expression {
public:
  // For AND/OR (binary)
  LogicalExpression(LogicalOpType op, std::unique_ptr<Expression> left,
                    std::unique_ptr<Expression> right);

  // For NOT (unary)
  LogicalExpression(LogicalOpType op, std::unique_ptr<Expression> operand);

  [[nodiscard]] TupleValue evaluate(const Tuple &tuple,
                                    const Schema &schema) const override;
  [[nodiscard]] TypeId result_type() const override;
  [[nodiscard]] std::unique_ptr<Expression> clone() const override;

  [[nodiscard]] LogicalOpType op() const noexcept { return op_; }

private:
  LogicalOpType op_;
  std::unique_ptr<Expression> left_;  // Also used as operand for NOT
  std::unique_ptr<Expression> right_; // nullptr for NOT
};

} // namespace entropy
