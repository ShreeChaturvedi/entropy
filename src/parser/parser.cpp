/**
 * @file parser.cpp
 * @brief Recursive descent SQL parser implementation
 */

#include "parser/parser.hpp"

namespace entropy {

Parser::Parser(std::string_view sql)
    : lexer_(sql), current_(lexer_.next_token()) {}

Status Parser::parse(std::unique_ptr<Statement> *result) {
  if (result == nullptr) {
    return Status::InvalidArgument("Result pointer cannot be null");
  }

  has_error_ = false;
  auto stmt = parse_statement();

  if (has_error_) {
    return Status::InvalidArgument(error_.to_string());
  }

  *result = std::move(stmt);
  return Status::Ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Token Helpers
// ─────────────────────────────────────────────────────────────────────────────

Token Parser::current_token() { return current_; }

Token Parser::peek_token() { return lexer_.peek_token(); }

void Parser::advance() { current_ = lexer_.next_token(); }

bool Parser::check(TokenType type) { return current_.type == type; }

bool Parser::match(TokenType type) {
  if (check(type)) {
    advance();
    return true;
  }
  return false;
}

void Parser::expect(TokenType type, const std::string &msg) {
  if (!match(type)) {
    set_error(msg + " (got: " +
              std::string(token_type_to_string(current_.type)) + ")");
  }
}

void Parser::set_error(const std::string &message) {
  if (!has_error_) {
    has_error_ = true;
    error_.message = message;
    error_.line = current_.line;
    error_.column = current_.column;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Statement Parsing
// ─────────────────────────────────────────────────────────────────────────────

std::unique_ptr<Statement> Parser::parse_statement() {
  if (check(TokenType::SELECT)) {
    return parse_select();
  }
  if (check(TokenType::INSERT)) {
    return parse_insert();
  }
  if (check(TokenType::UPDATE)) {
    return parse_update();
  }
  if (check(TokenType::DELETE)) {
    return parse_delete();
  }
  if (check(TokenType::CREATE)) {
    advance();
    if (check(TokenType::TABLE)) {
      return parse_create_table();
    }
    set_error("Expected TABLE after CREATE");
    return nullptr;
  }
  if (check(TokenType::DROP)) {
    advance();
    if (check(TokenType::TABLE)) {
      return parse_drop_table();
    }
    set_error("Expected TABLE after DROP");
    return nullptr;
  }

  set_error(
      "Expected statement (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP)");
  return nullptr;
}

std::unique_ptr<SelectStatement> Parser::parse_select() {
  auto stmt = std::make_unique<SelectStatement>();
  expect(TokenType::SELECT, "Expected SELECT");

  // Parse columns: * or column list
  if (match(TokenType::STAR)) {
    stmt->columns.push_back(SelectColumn::star());
  } else {
    do {
      if (check(TokenType::IDENTIFIER)) {
        stmt->columns.push_back(SelectColumn::column(current_.value));
        advance();
      } else {
        set_error("Expected column name");
        return nullptr;
      }
    } while (match(TokenType::COMMA));
  }

  // Parse FROM clause
  expect(TokenType::FROM, "Expected FROM");
  if (check(TokenType::IDENTIFIER)) {
    stmt->table.table_name = current_.value;
    advance();
    // Optional alias: FROM users u
    if (check(TokenType::IDENTIFIER) && !check(TokenType::WHERE) &&
        !check(TokenType::JOIN) && !check(TokenType::INNER) &&
        !check(TokenType::LEFT) && !check(TokenType::RIGHT) &&
        !check(TokenType::CROSS) && !check(TokenType::ORDER) &&
        !check(TokenType::LIMIT)) {
      stmt->table.alias = current_.value;
      advance();
    }
  } else {
    set_error("Expected table name after FROM");
    return nullptr;
  }

  // Parse optional JOIN clauses
  while (true) {
    JoinType join_type = JoinType::INNER;
    bool has_join = false;

    if (match(TokenType::INNER)) {
      join_type = JoinType::INNER;
      expect(TokenType::JOIN, "Expected JOIN after INNER");
      has_join = true;
    } else if (match(TokenType::LEFT)) {
      join_type = JoinType::LEFT;
      match(TokenType::OUTER); // Optional OUTER
      expect(TokenType::JOIN, "Expected JOIN after LEFT [OUTER]");
      has_join = true;
    } else if (match(TokenType::RIGHT)) {
      join_type = JoinType::RIGHT;
      match(TokenType::OUTER); // Optional OUTER
      expect(TokenType::JOIN, "Expected JOIN after RIGHT [OUTER]");
      has_join = true;
    } else if (match(TokenType::CROSS)) {
      join_type = JoinType::CROSS;
      expect(TokenType::JOIN, "Expected JOIN after CROSS");
      has_join = true;
    } else if (match(TokenType::JOIN)) {
      // Plain JOIN = INNER JOIN
      join_type = JoinType::INNER;
      has_join = true;
    }

    if (!has_join)
      break;

    JoinClause join;
    join.type = join_type;

    // Parse joined table
    if (check(TokenType::IDENTIFIER)) {
      join.table.table_name = current_.value;
      advance();
      // Optional alias
      if (check(TokenType::IDENTIFIER) && !check(TokenType::ON) &&
          !check(TokenType::WHERE)) {
        join.table.alias = current_.value;
        advance();
      }
    } else {
      set_error("Expected table name after JOIN");
      return nullptr;
    }

    // Parse ON condition (not required for CROSS JOIN)
    if (join_type != JoinType::CROSS) {
      expect(TokenType::ON, "Expected ON after JOIN table");
      join.condition = parse_expression();
    }

    stmt->joins.push_back(std::move(join));
  }

  // Parse optional WHERE clause
  if (match(TokenType::WHERE)) {
    stmt->where_clause = parse_expression();
  }

  // Parse optional ORDER BY clause
  if (match(TokenType::ORDER)) {
    expect(TokenType::BY, "Expected BY after ORDER");
    do {
      if (check(TokenType::IDENTIFIER)) {
        OrderByItem item;
        item.column_name = current_.value;
        advance();
        if (match(TokenType::DESC)) {
          item.ascending = false;
        } else {
          match(TokenType::ASC); // Optional ASC
        }
        stmt->order_by.push_back(item);
      } else {
        set_error("Expected column name in ORDER BY");
        return nullptr;
      }
    } while (match(TokenType::COMMA));
  }

  // Parse optional LIMIT clause
  if (match(TokenType::LIMIT)) {
    if (check(TokenType::INTEGER_LITERAL)) {
      stmt->limit = std::stoull(current_.value);
      advance();
    } else {
      set_error("Expected integer after LIMIT");
      return nullptr;
    }

    // Optional OFFSET
    if (match(TokenType::OFFSET)) {
      if (check(TokenType::INTEGER_LITERAL)) {
        stmt->offset = std::stoull(current_.value);
        advance();
      } else {
        set_error("Expected integer after OFFSET");
        return nullptr;
      }
    }
  }

  match(TokenType::SEMICOLON); // Optional trailing semicolon
  return stmt;
}

std::unique_ptr<InsertStatement> Parser::parse_insert() {
  auto stmt = std::make_unique<InsertStatement>();
  expect(TokenType::INSERT, "Expected INSERT");
  expect(TokenType::INTO, "Expected INTO");

  // Table name
  if (check(TokenType::IDENTIFIER)) {
    stmt->table_name = current_.value;
    advance();
  } else {
    set_error("Expected table name after INTO");
    return nullptr;
  }

  // Optional column list
  if (match(TokenType::LPAREN)) {
    do {
      if (check(TokenType::IDENTIFIER)) {
        stmt->columns.push_back(current_.value);
        advance();
      } else {
        set_error("Expected column name");
        return nullptr;
      }
    } while (match(TokenType::COMMA));
    expect(TokenType::RPAREN, "Expected )");
  }

  // VALUES clause
  expect(TokenType::VALUES, "Expected VALUES");

  // Parse value rows
  do {
    expect(TokenType::LPAREN, "Expected (");
    std::vector<InsertValue> row;

    do {
      if (check(TokenType::INTEGER_LITERAL)) {
        row.push_back(std::stoll(current_.value));
        advance();
      } else if (check(TokenType::FLOAT_LITERAL)) {
        row.push_back(std::stod(current_.value));
        advance();
      } else if (check(TokenType::STRING_LITERAL)) {
        row.push_back(current_.value);
        advance();
      } else if (check(TokenType::TRUE_KEYWORD)) {
        row.push_back(true);
        advance();
      } else if (check(TokenType::FALSE_KEYWORD)) {
        row.push_back(false);
        advance();
      } else if (check(TokenType::NULL_KEYWORD)) {
        row.push_back(std::monostate{});
        advance();
      } else {
        set_error("Expected value in VALUES");
        return nullptr;
      }
    } while (match(TokenType::COMMA));

    expect(TokenType::RPAREN, "Expected )");
    stmt->values.push_back(std::move(row));
  } while (match(TokenType::COMMA));

  match(TokenType::SEMICOLON);
  return stmt;
}

std::unique_ptr<UpdateStatement> Parser::parse_update() {
  auto stmt = std::make_unique<UpdateStatement>();
  expect(TokenType::UPDATE, "Expected UPDATE");

  // Table name
  if (check(TokenType::IDENTIFIER)) {
    stmt->table_name = current_.value;
    advance();
  } else {
    set_error("Expected table name after UPDATE");
    return nullptr;
  }

  // SET clause
  expect(TokenType::SET, "Expected SET");

  do {
    SetClause clause;
    if (check(TokenType::IDENTIFIER)) {
      clause.column_name = current_.value;
      advance();
    } else {
      set_error("Expected column name in SET");
      return nullptr;
    }

    expect(TokenType::EQ, "Expected = in SET clause");
    clause.value = parse_expression();
    stmt->set_clauses.push_back(std::move(clause));
  } while (match(TokenType::COMMA));

  // Optional WHERE clause
  if (match(TokenType::WHERE)) {
    stmt->where_clause = parse_expression();
  }

  match(TokenType::SEMICOLON);
  return stmt;
}

std::unique_ptr<DeleteStatement> Parser::parse_delete() {
  auto stmt = std::make_unique<DeleteStatement>();
  expect(TokenType::DELETE, "Expected DELETE");
  expect(TokenType::FROM, "Expected FROM");

  // Table name
  if (check(TokenType::IDENTIFIER)) {
    stmt->table_name = current_.value;
    advance();
  } else {
    set_error("Expected table name after FROM");
    return nullptr;
  }

  // Optional WHERE clause
  if (match(TokenType::WHERE)) {
    stmt->where_clause = parse_expression();
  }

  match(TokenType::SEMICOLON);
  return stmt;
}

std::unique_ptr<CreateTableStatement> Parser::parse_create_table() {
  auto stmt = std::make_unique<CreateTableStatement>();
  expect(TokenType::TABLE, "Expected TABLE");

  // Table name
  if (check(TokenType::IDENTIFIER)) {
    stmt->table_name = current_.value;
    advance();
  } else {
    set_error("Expected table name after CREATE TABLE");
    return nullptr;
  }

  // Column definitions
  expect(TokenType::LPAREN, "Expected (");

  do {
    ColumnDef col;

    // Column name
    if (check(TokenType::IDENTIFIER)) {
      col.name = current_.value;
      advance();
    } else {
      set_error("Expected column name");
      return nullptr;
    }

    // Data type
    col.type = parse_data_type(&col.length);

    // Optional PRIMARY KEY
    if (check(TokenType::PRIMARY)) {
      advance();
      expect(TokenType::KEY, "Expected KEY after PRIMARY");
      col.is_primary_key = true;
    }

    // Optional NOT NULL
    if (check(TokenType::NOT)) {
      advance();
      expect(TokenType::NULL_KEYWORD, "Expected NULL after NOT");
      col.nullable = false;
    }

    stmt->columns.push_back(col);
  } while (match(TokenType::COMMA));

  expect(TokenType::RPAREN, "Expected )");
  match(TokenType::SEMICOLON);
  return stmt;
}

std::unique_ptr<DropTableStatement> Parser::parse_drop_table() {
  auto stmt = std::make_unique<DropTableStatement>();
  expect(TokenType::TABLE, "Expected TABLE");

  // Table name
  if (check(TokenType::IDENTIFIER)) {
    stmt->table_name = current_.value;
    advance();
  } else {
    set_error("Expected table name after DROP TABLE");
    return nullptr;
  }

  match(TokenType::SEMICOLON);
  return stmt;
}

// ─────────────────────────────────────────────────────────────────────────────
// Expression Parsing (precedence climbing)
// ─────────────────────────────────────────────────────────────────────────────

std::unique_ptr<Expression> Parser::parse_expression() {
  return parse_or_expression();
}

std::unique_ptr<Expression> Parser::parse_or_expression() {
  auto left = parse_and_expression();

  while (match(TokenType::OR)) {
    auto right = parse_and_expression();
    left = std::make_unique<LogicalExpression>(
        LogicalOpType::OR, std::move(left), std::move(right));
  }

  return left;
}

std::unique_ptr<Expression> Parser::parse_and_expression() {
  auto left = parse_comparison();

  while (match(TokenType::AND)) {
    auto right = parse_comparison();
    left = std::make_unique<LogicalExpression>(
        LogicalOpType::AND, std::move(left), std::move(right));
  }

  return left;
}

std::unique_ptr<Expression> Parser::parse_comparison() {
  auto left = parse_additive();

  if (check(TokenType::EQ) || check(TokenType::NE) || check(TokenType::LT) ||
      check(TokenType::LE) || check(TokenType::GT) || check(TokenType::GE)) {

    ComparisonType cmp;
    if (current_.type == TokenType::EQ)
      cmp = ComparisonType::EQUAL;
    else if (current_.type == TokenType::NE)
      cmp = ComparisonType::NOT_EQUAL;
    else if (current_.type == TokenType::LT)
      cmp = ComparisonType::LESS_THAN;
    else if (current_.type == TokenType::LE)
      cmp = ComparisonType::LESS_EQUAL;
    else if (current_.type == TokenType::GT)
      cmp = ComparisonType::GREATER_THAN;
    else
      cmp = ComparisonType::GREATER_EQUAL;

    advance();
    auto right = parse_additive();
    left = std::make_unique<ComparisonExpression>(cmp, std::move(left),
                                                  std::move(right));
  }

  return left;
}

std::unique_ptr<Expression> Parser::parse_additive() {
  auto left = parse_multiplicative();

  while (check(TokenType::PLUS) || check(TokenType::MINUS)) {
    BinaryOpType op = (current_.type == TokenType::PLUS)
                          ? BinaryOpType::ADD
                          : BinaryOpType::SUBTRACT;
    advance();
    auto right = parse_multiplicative();
    left = std::make_unique<BinaryOpExpression>(op, std::move(left),
                                                std::move(right));
  }

  return left;
}

std::unique_ptr<Expression> Parser::parse_multiplicative() {
  auto left = parse_unary();

  while (check(TokenType::STAR) || check(TokenType::SLASH)) {
    BinaryOpType op = (current_.type == TokenType::STAR)
                          ? BinaryOpType::MULTIPLY
                          : BinaryOpType::DIVIDE;
    advance();
    auto right = parse_unary();
    left = std::make_unique<BinaryOpExpression>(op, std::move(left),
                                                std::move(right));
  }

  return left;
}

std::unique_ptr<Expression> Parser::parse_unary() {
  if (match(TokenType::NOT)) {
    auto operand = parse_unary();
    return std::make_unique<LogicalExpression>(LogicalOpType::NOT,
                                               std::move(operand));
  }

  if (match(TokenType::MINUS)) {
    auto operand = parse_unary();
    // Create: 0 - operand
    auto zero = std::make_unique<ConstantExpression>(
        TupleValue(static_cast<int64_t>(0)));
    return std::make_unique<BinaryOpExpression>(
        BinaryOpType::SUBTRACT, std::move(zero), std::move(operand));
  }

  return parse_primary();
}

std::unique_ptr<Expression> Parser::parse_primary() {
  // Integer literal
  if (check(TokenType::INTEGER_LITERAL)) {
    auto val = TupleValue(std::stoll(current_.value));
    advance();
    return std::make_unique<ConstantExpression>(val);
  }

  // Float literal
  if (check(TokenType::FLOAT_LITERAL)) {
    auto val = TupleValue(std::stod(current_.value));
    advance();
    return std::make_unique<ConstantExpression>(val);
  }

  // String literal
  if (check(TokenType::STRING_LITERAL)) {
    auto val = TupleValue(current_.value);
    advance();
    return std::make_unique<ConstantExpression>(val);
  }

  // Boolean literals
  if (check(TokenType::TRUE_KEYWORD)) {
    advance();
    return std::make_unique<ConstantExpression>(TupleValue(true));
  }
  if (check(TokenType::FALSE_KEYWORD)) {
    advance();
    return std::make_unique<ConstantExpression>(TupleValue(false));
  }

  // NULL
  if (check(TokenType::NULL_KEYWORD)) {
    advance();
    return std::make_unique<ConstantExpression>(TupleValue());
  }

  // Column reference (identifier)
  if (check(TokenType::IDENTIFIER)) {
    std::string name = current_.value;
    advance();

    // Check for table.column
    if (match(TokenType::DOT)) {
      if (check(TokenType::IDENTIFIER)) {
        std::string col_name = current_.value;
        advance();
        return std::make_unique<ColumnRefExpression>(col_name, name);
      }
      set_error("Expected column name after .");
      return nullptr;
    }

    return std::make_unique<ColumnRefExpression>(name);
  }

  // Parenthesized expression
  if (match(TokenType::LPAREN)) {
    auto expr = parse_expression();
    expect(TokenType::RPAREN, "Expected )");
    return expr;
  }

  set_error("Expected expression");
  return nullptr;
}

// ─────────────────────────────────────────────────────────────────────────────
// Type Parsing
// ─────────────────────────────────────────────────────────────────────────────

TypeId Parser::parse_data_type(size_t *length_out) {
  TypeId type = TypeId::INVALID;

  if (match(TokenType::INT) || match(TokenType::INTEGER)) {
    type = TypeId::INTEGER;
  } else if (match(TokenType::BIGINT)) {
    type = TypeId::BIGINT;
  } else if (match(TokenType::SMALLINT)) {
    type = TypeId::SMALLINT;
  } else if (match(TokenType::BOOLEAN)) {
    type = TypeId::BOOLEAN;
  } else if (match(TokenType::FLOAT)) {
    type = TypeId::FLOAT;
  } else if (match(TokenType::DOUBLE)) {
    type = TypeId::DOUBLE;
  } else if (match(TokenType::VARCHAR) || match(TokenType::TEXT)) {
    type = TypeId::VARCHAR;
    // Parse optional length: VARCHAR(100)
    if (match(TokenType::LPAREN)) {
      if (check(TokenType::INTEGER_LITERAL)) {
        if (length_out) {
          *length_out = std::stoull(current_.value);
        }
        advance();
      }
      expect(TokenType::RPAREN, "Expected )");
    } else if (length_out) {
      *length_out = 255; // Default VARCHAR length
    }
  } else {
    set_error("Expected data type");
  }

  return type;
}

} // namespace entropy
