/**
 * @file binder.cpp
 * @brief Semantic analysis implementation
 */

#include "parser/binder.hpp"

namespace entropy {

namespace {

/// True if the SQL type belongs to the numeric family (ints and floats are
/// mutually assignable with widening/narrowing conversions).
[[nodiscard]] bool is_numeric_type(TypeId type) {
  switch (type) {
  case TypeId::TINYINT:
  case TypeId::SMALLINT:
  case TypeId::INTEGER:
  case TypeId::BIGINT:
  case TypeId::DECIMAL:
  case TypeId::FLOAT:
  case TypeId::DOUBLE:
    return true;
  default:
    return false;
  }
}

/// Human-readable kind of an INSERT literal, for strict-mode error messages.
[[nodiscard]] const char *insert_value_kind(const InsertValue &value) {
  if (std::holds_alternative<std::monostate>(value)) return "NULL";
  if (std::holds_alternative<bool>(value)) return "boolean";
  if (std::holds_alternative<int64_t>(value)) return "integer";
  if (std::holds_alternative<double>(value)) return "floating-point";
  return "string";
}

/// Whether a parsed literal may be stored in a column of @p target without a
/// cross-family coercion. NULL is always allowed; numerics are one family.
[[nodiscard]] bool insert_value_matches(const InsertValue &value,
                                        TypeId target) {
  if (std::holds_alternative<std::monostate>(value)) {
    return true;
  }
  if (std::holds_alternative<bool>(value)) {
    return target == TypeId::BOOLEAN;
  }
  if (std::holds_alternative<int64_t>(value) ||
      std::holds_alternative<double>(value)) {
    return is_numeric_type(target);
  }
  // string
  return target == TypeId::VARCHAR;
}

} // namespace

Binder::Binder(Catalog *catalog, bool strict_mode)
    : catalog_(catalog), strict_mode_(strict_mode) {}

// ─────────────────────────────────────────────────────────────────────────────
// Table/Column Resolution
// ─────────────────────────────────────────────────────────────────────────────

Status Binder::resolve_table(const std::string &table_name,
                             std::shared_ptr<TableInfo> *out) {
  std::shared_ptr<TableInfo> info = catalog_->get_table_shared(table_name);
  if (info == nullptr) {
    return Status::NotFound("Table not found: " + table_name);
  }
  *out = std::move(info);
  return Status::Ok();
}

Status Binder::resolve_column(const std::string &column_name,
                              const Schema &schema, size_t *out_index,
                              TypeId *out_type) {
  int idx = schema.get_column_index(column_name);
  if (idx < 0) {
    return Status::NotFound("Column not found: " + column_name);
  }
  *out_index = static_cast<size_t>(idx);
  *out_type = schema.column(static_cast<size_t>(idx)).type();
  return Status::Ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Expression Binding
// ─────────────────────────────────────────────────────────────────────────────

Status Binder::bind_expression(Expression *expr, const TableInfo *table_info) {
  if (expr == nullptr) {
    return Status::Ok();
  }

  switch (expr->expr_type()) {
  case ExpressionType::CONSTANT:
    // Constants need no binding
    return Status::Ok();

  case ExpressionType::COLUMN_REF: {
    auto *col_ref = dynamic_cast<ColumnRefExpression *>(expr);
    if (col_ref == nullptr) {
      return Status::InvalidArgument("Invalid column reference expression");
    }

    size_t idx;
    TypeId type;
    auto status =
        resolve_column(col_ref->column_name(), table_info->schema, &idx, &type);
    if (!status.ok()) {
      return status;
    }

    col_ref->set_column_index(idx);
    col_ref->set_type(type);
    return Status::Ok();
  }

  case ExpressionType::BINARY_OP: {
    auto *binary = dynamic_cast<BinaryOpExpression *>(expr);
    if (binary == nullptr) {
      return Status::InvalidArgument("Invalid binary expression");
    }
    auto status =
        bind_expression(const_cast<Expression *>(binary->left()), table_info);
    if (!status.ok())
      return status;
    return bind_expression(const_cast<Expression *>(binary->right()),
                           table_info);
  }

  case ExpressionType::COMPARISON: {
    auto *cmp = dynamic_cast<ComparisonExpression *>(expr);
    if (cmp == nullptr) {
      return Status::InvalidArgument("Invalid comparison expression");
    }
    auto status =
        bind_expression(const_cast<Expression *>(cmp->left()), table_info);
    if (!status.ok())
      return status;
    return bind_expression(const_cast<Expression *>(cmp->right()), table_info);
  }

  case ExpressionType::LOGICAL: {
    auto *logical = dynamic_cast<LogicalExpression *>(expr);
    if (logical == nullptr) {
      return Status::InvalidArgument("Invalid logical expression");
    }
    // AND/OR are binary; NOT is unary with left() as its operand and a null
    // right(). Recurse into both children so columns under AND/OR/NOT get
    // resolved and unknown columns surface as a clean NotFound (bind_expression
    // treats a null child as Ok).
    auto status =
        bind_expression(const_cast<Expression *>(logical->left()), table_info);
    if (!status.ok())
      return status;
    return bind_expression(const_cast<Expression *>(logical->right()),
                           table_info);
  }

  case ExpressionType::IS_NULL: {
    auto *is_null = dynamic_cast<IsNullExpression *>(expr);
    if (is_null == nullptr) {
      return Status::InvalidArgument("Invalid IS NULL expression");
    }
    return bind_expression(const_cast<Expression *>(is_null->operand()),
                           table_info);
  }

  case ExpressionType::UNARY:
    // Unary minus lowers to a BinaryOpExpression and NOT to a
    // LogicalExpression, so there are no children to bind here.
    return Status::Ok();
  }

  return Status::Ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Statement Binding
// ─────────────────────────────────────────────────────────────────────────────

Status Binder::bind_select(SelectStatement *stmt, BoundSelectContext *context) {
  // Resolve table
  auto status = resolve_table(stmt->table.table_name, &context->table_info);
  if (!status.ok()) {
    return status;
  }

  const Schema &schema = context->table_info->schema;

  // Resolve columns
  if (stmt->columns.size() == 1 && stmt->columns[0].is_star) {
    context->select_all = true;
    // All columns
    for (size_t i = 0; i < schema.column_count(); i++) {
      context->column_indices.push_back(i);
    }
  } else {
    context->select_all = false;
    for (const auto &col : stmt->columns) {
      size_t idx;
      TypeId type;
      status = resolve_column(col.column_name, schema, &idx, &type);
      if (!status.ok()) {
        return status;
      }
      context->column_indices.push_back(idx);
    }
  }

  // Bind WHERE clause
  if (stmt->where_clause) {
    status = bind_expression(stmt->where_clause.get(), context->table_info.get());
    if (!status.ok()) {
      return status;
    }
    context->predicate = std::move(stmt->where_clause);
  }

  return Status::Ok();
}

Status Binder::bind_insert(InsertStatement *stmt, BoundInsertContext *context) {
  // Resolve table
  auto status = resolve_table(stmt->table_name, &context->table_info);
  if (!status.ok()) {
    return status;
  }

  const Schema &schema = context->table_info->schema;

  // Resolve columns
  if (stmt->columns.empty()) {
    // Insert into all columns in order
    for (size_t i = 0; i < schema.column_count(); i++) {
      context->column_indices.push_back(i);
    }
  } else {
    for (const auto &col_name : stmt->columns) {
      size_t idx;
      TypeId type;
      status = resolve_column(col_name, schema, &idx, &type);
      if (!status.ok()) {
        return status;
      }
      context->column_indices.push_back(idx);
    }
  }

  // Validate that each row has the right number of values
  for (const auto &row : stmt->values) {
    if (row.size() != context->column_indices.size()) {
      return Status::InvalidArgument(
          "INSERT value count (" + std::to_string(row.size()) +
          ") doesn't match column count (" +
          std::to_string(context->column_indices.size()) + ")");
    }

    // Strict mode: reject literals whose kind does not belong to the target
    // column's type family instead of silently coercing them.
    if (strict_mode_) {
      for (size_t i = 0; i < row.size(); i++) {
        const Column &column = schema.column(context->column_indices[i]);
        if (!insert_value_matches(row[i], column.type())) {
          return Status::InvalidArgument(
              std::string("strict mode: cannot insert ") +
              insert_value_kind(row[i]) + " value into column '" +
              column.name() + "'");
        }
      }
    }
  }

  return Status::Ok();
}

Status Binder::bind_update(UpdateStatement *stmt, BoundUpdateContext *context) {
  // Resolve table
  auto status = resolve_table(stmt->table_name, &context->table_info);
  if (!status.ok()) {
    return status;
  }

  const Schema &schema = context->table_info->schema;

  // Resolve SET clause columns and bind expressions
  for (auto &set_clause : stmt->set_clauses) {
    size_t idx;
    TypeId type;
    status = resolve_column(set_clause.column_name, schema, &idx, &type);
    if (!status.ok()) {
      return status;
    }
    context->column_indices.push_back(idx);

    // Bind the value expression
    if (set_clause.value) {
      status = bind_expression(set_clause.value.get(), context->table_info.get());
      if (!status.ok()) {
        return status;
      }
      context->values.push_back(std::move(set_clause.value));
    }
  }

  // Bind WHERE clause
  if (stmt->where_clause) {
    status = bind_expression(stmt->where_clause.get(), context->table_info.get());
    if (!status.ok()) {
      return status;
    }
    context->predicate = std::move(stmt->where_clause);
  }

  return Status::Ok();
}

Status Binder::bind_delete(DeleteStatement *stmt, BoundDeleteContext *context) {
  // Resolve table
  auto status = resolve_table(stmt->table_name, &context->table_info);
  if (!status.ok()) {
    return status;
  }

  // Bind WHERE clause
  if (stmt->where_clause) {
    status = bind_expression(stmt->where_clause.get(), context->table_info.get());
    if (!status.ok()) {
      return status;
    }
    context->predicate = std::move(stmt->where_clause);
  }

  return Status::Ok();
}

} // namespace entropy
