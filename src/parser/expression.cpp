/**
 * @file expression.cpp
 * @brief Expression evaluation implementation
 */

#include "parser/expression.hpp"

#include <limits>

#include "catalog/schema.hpp"

namespace entropy {

// Helper to determine TypeId from TupleValue
static TypeId get_tuple_value_type(const TupleValue &val) {
  if (val.is_null())
    return TypeId::INVALID;
  if (val.is_bool())
    return TypeId::BOOLEAN;
  if (val.is_tinyint())
    return TypeId::TINYINT;
  if (val.is_smallint())
    return TypeId::SMALLINT;
  if (val.is_integer())
    return TypeId::INTEGER;
  if (val.is_bigint())
    return TypeId::BIGINT;
  if (val.is_float())
    return TypeId::FLOAT;
  if (val.is_double())
    return TypeId::DOUBLE;
  if (val.is_string())
    return TypeId::VARCHAR;
  return TypeId::INVALID;
}

// Helper to convert to int64_t for arithmetic
static int64_t to_int64(const TupleValue &val) {
  if (val.is_tinyint())
    return val.as_tinyint();
  if (val.is_smallint())
    return val.as_smallint();
  if (val.is_integer())
    return val.as_integer();
  if (val.is_bigint())
    return val.as_bigint();
  return 0;
}

// Helper to convert to double for arithmetic
static double to_double(const TupleValue &val) {
  if (val.is_float())
    return static_cast<double>(val.as_float());
  if (val.is_double())
    return val.as_double();
  if (val.is_tinyint())
    return val.as_tinyint();
  if (val.is_smallint())
    return val.as_smallint();
  if (val.is_integer())
    return val.as_integer();
  if (val.is_bigint())
    return static_cast<double>(val.as_bigint());
  return 0.0;
}

// Helper to check if value is numeric integer type
static bool is_integer_type(const TupleValue &val) {
  return val.is_tinyint() || val.is_smallint() || val.is_integer() ||
         val.is_bigint();
}

// Helper to check if value is floating point
static bool is_float_type(const TupleValue &val) {
  return val.is_float() || val.is_double();
}

// Helper to check if value is any numeric type (integer or floating point)
static bool is_numeric_value(const TupleValue &val) {
  return is_integer_type(val) || is_float_type(val);
}

// ─────────────────────────────────────────────────────────────────────────────
// Checked 64-bit integer arithmetic
//
// Signed integer overflow is undefined behaviour in C++. These helpers detect
// overflow before it happens and report it via the return value so callers can
// surface a defined result instead of tripping UB (or -fsanitize=undefined).
// They use the compiler overflow builtins where available and fall back to a
// portable pre-check otherwise.
// ─────────────────────────────────────────────────────────────────────────────

static bool checked_add(int64_t a, int64_t b, int64_t &out) {
#if defined(__GNUC__) || defined(__clang__)
  return __builtin_add_overflow(a, b, &out);
#else
  if ((b > 0 && a > std::numeric_limits<int64_t>::max() - b) ||
      (b < 0 && a < std::numeric_limits<int64_t>::min() - b)) {
    return true;
  }
  out = a + b;
  return false;
#endif
}

static bool checked_sub(int64_t a, int64_t b, int64_t &out) {
#if defined(__GNUC__) || defined(__clang__)
  return __builtin_sub_overflow(a, b, &out);
#else
  if ((b < 0 && a > std::numeric_limits<int64_t>::max() + b) ||
      (b > 0 && a < std::numeric_limits<int64_t>::min() + b)) {
    return true;
  }
  out = a - b;
  return false;
#endif
}

static bool checked_mul(int64_t a, int64_t b, int64_t &out) {
#if defined(__GNUC__) || defined(__clang__)
  return __builtin_mul_overflow(a, b, &out);
#else
  if (a == 0 || b == 0) {
    out = 0;
    return false;
  }
  const int64_t result = a * b;
  // Recover an operand by division; a mismatch means the product wrapped.
  if (result / b != a) {
    return true;
  }
  out = result;
  return false;
#endif
}

// ─────────────────────────────────────────────────────────────────────────────
// ConstantExpression
// ─────────────────────────────────────────────────────────────────────────────

ConstantExpression::ConstantExpression(TupleValue value)
    : Expression(ExpressionType::CONSTANT), value_(std::move(value)) {}

TupleValue ConstantExpression::evaluate(const Tuple & /*tuple*/,
                                        const Schema & /*schema*/) const {
  return value_;
}

TypeId ConstantExpression::result_type() const {
  return get_tuple_value_type(value_);
}

std::unique_ptr<Expression> ConstantExpression::clone() const {
  return std::make_unique<ConstantExpression>(value_);
}

// ─────────────────────────────────────────────────────────────────────────────
// ColumnRefExpression
// ─────────────────────────────────────────────────────────────────────────────

ColumnRefExpression::ColumnRefExpression(std::string column_name,
                                         std::string table_name)
    : Expression(ExpressionType::COLUMN_REF),
      column_name_(std::move(column_name)), table_name_(std::move(table_name)) {
}

TupleValue ColumnRefExpression::evaluate(const Tuple &tuple,
                                         const Schema &schema) const {
  // After binding, column_idx_ should be set
  if (type_ != TypeId::INVALID) {
    return tuple.get_value(schema, static_cast<uint32_t>(column_idx_));
  }

  // Fallback: find by name
  int idx = schema.get_column_index(column_name_);
  if (idx >= 0) {
    return tuple.get_value(schema, static_cast<uint32_t>(idx));
  }

  // Column not found - return null
  return TupleValue();
}

TypeId ColumnRefExpression::result_type() const { return type_; }

std::unique_ptr<Expression> ColumnRefExpression::clone() const {
  auto expr = std::make_unique<ColumnRefExpression>(column_name_, table_name_);
  expr->column_idx_ = column_idx_;
  expr->type_ = type_;
  return expr;
}

// ─────────────────────────────────────────────────────────────────────────────
// BinaryOpExpression
// ─────────────────────────────────────────────────────────────────────────────

BinaryOpExpression::BinaryOpExpression(BinaryOpType op,
                                       std::unique_ptr<Expression> left,
                                       std::unique_ptr<Expression> right)
    : Expression(ExpressionType::BINARY_OP), op_(op), left_(std::move(left)),
      right_(std::move(right)) {}

TupleValue BinaryOpExpression::evaluate(const Tuple &tuple,
                                        const Schema &schema) const {
  TupleValue lval = left_->evaluate(tuple, schema);
  TupleValue rval = right_->evaluate(tuple, schema);

  // Handle null values
  if (lval.is_null() || rval.is_null()) {
    return TupleValue(); // NULL
  }

  // Integer operations: use checked arithmetic so overflow yields a defined
  // NULL result (matching division-by-zero below) instead of undefined
  // behaviour.
  if (is_integer_type(lval) && is_integer_type(rval)) {
    int64_t l = to_int64(lval);
    int64_t r = to_int64(rval);
    int64_t out = 0;

    switch (op_) {
    case BinaryOpType::ADD:
      return checked_add(l, r, out) ? TupleValue() : TupleValue(out);
    case BinaryOpType::SUBTRACT:
      return checked_sub(l, r, out) ? TupleValue() : TupleValue(out);
    case BinaryOpType::MULTIPLY:
      return checked_mul(l, r, out) ? TupleValue() : TupleValue(out);
    case BinaryOpType::DIVIDE:
      if (r == 0)
        return TupleValue(); // NULL on division by zero
      // INT64_MIN / -1 overflows the signed range and traps: guard it.
      if (l == std::numeric_limits<int64_t>::min() && r == -1)
        return TupleValue();
      return TupleValue(l / r);
    }
  }

  // Floating-point operations: only when BOTH operands are numeric. A
  // non-numeric operand (e.g. a string) has no arithmetic meaning and is NOT
  // silently coerced to 0 — bind-time checking rejects such expressions, and
  // this evaluation path returns NULL as a defensive fallback.
  if (is_numeric_value(lval) && is_numeric_value(rval)) {
    double l = to_double(lval);
    double r = to_double(rval);

    switch (op_) {
    case BinaryOpType::ADD:
      return TupleValue(l + r);
    case BinaryOpType::SUBTRACT:
      return TupleValue(l - r);
    case BinaryOpType::MULTIPLY:
      return TupleValue(l * r);
    case BinaryOpType::DIVIDE:
      if (r == 0.0)
        return TupleValue();
      return TupleValue(l / r);
    }
  }

  return TupleValue(); // Non-numeric operands: no coercion, result is NULL
}

TypeId BinaryOpExpression::result_type() const {
  TypeId left_type = left_->result_type();
  TypeId right_type = right_->result_type();

  // Promote to DOUBLE if either operand is floating point
  if (left_type == TypeId::DOUBLE || left_type == TypeId::FLOAT ||
      right_type == TypeId::DOUBLE || right_type == TypeId::FLOAT) {
    return TypeId::DOUBLE;
  }

  return TypeId::BIGINT;
}

std::unique_ptr<Expression> BinaryOpExpression::clone() const {
  return std::make_unique<BinaryOpExpression>(op_, left_->clone(),
                                              right_->clone());
}

// ─────────────────────────────────────────────────────────────────────────────
// ComparisonExpression
// ─────────────────────────────────────────────────────────────────────────────

ComparisonExpression::ComparisonExpression(ComparisonType cmp,
                                           std::unique_ptr<Expression> left,
                                           std::unique_ptr<Expression> right)
    : Expression(ExpressionType::COMPARISON), cmp_(cmp), left_(std::move(left)),
      right_(std::move(right)) {}

TupleValue ComparisonExpression::evaluate(const Tuple &tuple,
                                          const Schema &schema) const {
  TupleValue lval = left_->evaluate(tuple, schema);
  TupleValue rval = right_->evaluate(tuple, schema);

  // NULL comparisons return NULL (except IS NULL)
  if (lval.is_null() || rval.is_null()) {
    return TupleValue();
  }

  int cmp_result = 0;

  // Compare based on type
  if (is_integer_type(lval) && is_integer_type(rval)) {
    int64_t l = to_int64(lval);
    int64_t r = to_int64(rval);
    cmp_result = (l < r) ? -1 : (l > r) ? 1 : 0;
  } else if (is_numeric_value(lval) && is_numeric_value(rval)) {
    // Mixed integer/float comparison. Both operands are numeric; a non-numeric
    // operand is NOT coerced to 0 but falls through to the incompatible-type
    // case below (SQL unknown / NULL).
    double l = to_double(lval);
    double r = to_double(rval);
    cmp_result = (l < r) ? -1 : (l > r) ? 1 : 0;
  } else if (lval.is_string() && rval.is_string()) {
    cmp_result = lval.as_string().compare(rval.as_string());
  } else if (lval.is_bool() && rval.is_bool()) {
    bool l = lval.as_bool();
    bool r = rval.as_bool();
    cmp_result = (l == r) ? 0 : (l ? 1 : -1);
  } else {
    // Incompatible operand types have no defined ordering. Under SQL
    // three-valued logic the comparison is NULL (unknown), which downstream
    // filters treat as not-true, rather than falling through to cmp_result 0
    // and matching every row.
    return TupleValue::null();
  }

  bool result = false;
  switch (cmp_) {
  case ComparisonType::EQUAL:
    result = (cmp_result == 0);
    break;
  case ComparisonType::NOT_EQUAL:
    result = (cmp_result != 0);
    break;
  case ComparisonType::LESS_THAN:
    result = (cmp_result < 0);
    break;
  case ComparisonType::LESS_EQUAL:
    result = (cmp_result <= 0);
    break;
  case ComparisonType::GREATER_THAN:
    result = (cmp_result > 0);
    break;
  case ComparisonType::GREATER_EQUAL:
    result = (cmp_result >= 0);
    break;
  }

  return TupleValue(result);
}

TypeId ComparisonExpression::result_type() const { return TypeId::BOOLEAN; }

std::unique_ptr<Expression> ComparisonExpression::clone() const {
  return std::make_unique<ComparisonExpression>(cmp_, left_->clone(),
                                                right_->clone());
}

// ─────────────────────────────────────────────────────────────────────────────
// LogicalExpression
// ─────────────────────────────────────────────────────────────────────────────

LogicalExpression::LogicalExpression(LogicalOpType op,
                                     std::unique_ptr<Expression> left,
                                     std::unique_ptr<Expression> right)
    : Expression(ExpressionType::LOGICAL), op_(op), left_(std::move(left)),
      right_(std::move(right)) {}

LogicalExpression::LogicalExpression(LogicalOpType op,
                                     std::unique_ptr<Expression> operand)
    : Expression(ExpressionType::LOGICAL), op_(op), left_(std::move(operand)),
      right_(nullptr) {}

TupleValue LogicalExpression::evaluate(const Tuple &tuple,
                                       const Schema &schema) const {
  TupleValue lval = left_->evaluate(tuple, schema);

  switch (op_) {
  case LogicalOpType::NOT: {
    if (lval.is_null())
      return TupleValue();
    return TupleValue(!lval.as_bool());
  }

  case LogicalOpType::AND: {
    // Short-circuit: if left is false, result is false
    if (!lval.is_null() && !lval.as_bool()) {
      return TupleValue(false);
    }
    TupleValue rval = right_->evaluate(tuple, schema);
    if (lval.is_null() || rval.is_null()) {
      if (!lval.is_null() && !lval.as_bool()) {
        return TupleValue(false);
      }
      if (!rval.is_null() && !rval.as_bool()) {
        return TupleValue(false);
      }
      return TupleValue();
    }
    return TupleValue(lval.as_bool() && rval.as_bool());
  }

  case LogicalOpType::OR: {
    // Short-circuit: if left is true, result is true
    if (!lval.is_null() && lval.as_bool()) {
      return TupleValue(true);
    }
    TupleValue rval = right_->evaluate(tuple, schema);
    if (lval.is_null() || rval.is_null()) {
      if (!lval.is_null() && lval.as_bool()) {
        return TupleValue(true);
      }
      if (!rval.is_null() && rval.as_bool()) {
        return TupleValue(true);
      }
      return TupleValue();
    }
    return TupleValue(lval.as_bool() || rval.as_bool());
  }
  }

  return TupleValue();
}

TypeId LogicalExpression::result_type() const { return TypeId::BOOLEAN; }

std::unique_ptr<Expression> LogicalExpression::clone() const {
  if (op_ == LogicalOpType::NOT) {
    return std::make_unique<LogicalExpression>(op_, left_->clone());
  }
  return std::make_unique<LogicalExpression>(op_, left_->clone(),
                                             right_->clone());
}

// ─────────────────────────────────────────────────────────────────────────────
// IsNullExpression
// ─────────────────────────────────────────────────────────────────────────────

IsNullExpression::IsNullExpression(std::unique_ptr<Expression> operand,
                                   bool negated)
    : Expression(ExpressionType::IS_NULL), operand_(std::move(operand)),
      negated_(negated) {}

TupleValue IsNullExpression::evaluate(const Tuple &tuple,
                                      const Schema &schema) const {
  TupleValue val = operand_->evaluate(tuple, schema);
  bool is_null = val.is_null();
  return TupleValue(negated_ ? !is_null : is_null);
}

TypeId IsNullExpression::result_type() const { return TypeId::BOOLEAN; }

std::unique_ptr<Expression> IsNullExpression::clone() const {
  return std::make_unique<IsNullExpression>(operand_->clone(), negated_);
}

} // namespace entropy
