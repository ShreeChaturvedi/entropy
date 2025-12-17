#pragma once

/**
 * @file parser.hpp
 * @brief SQL Parser - recursive descent parser for SQL statements
 *
 * Supports:
 * - SELECT ... FROM ... WHERE ... ORDER BY ... LIMIT
 * - INSERT INTO ... VALUES ...
 * - UPDATE ... SET ... WHERE ...
 * - DELETE FROM ... WHERE ...
 * - CREATE TABLE ... (column definitions)
 * - DROP TABLE ...
 */

#include <memory>
#include <string_view>

#include "entropy/status.hpp"
#include "parser/expression.hpp"
#include "parser/statement.hpp"
#include "parser/token.hpp"

namespace entropy {

/**
 * @brief Parse error information
 */
struct ParseError {
  std::string message;
  size_t line = 0;
  size_t column = 0;

  [[nodiscard]] std::string to_string() const {
    return "Parse error at line " + std::to_string(line) + ", column " +
           std::to_string(column) + ": " + message;
  }
};

/**
 * @brief SQL Parser - converts SQL text to AST
 *
 * Usage:
 *   Parser parser("SELECT * FROM users WHERE id = 1");
 *   auto result = parser.parse();
 *   if (result.ok()) {
 *       auto& stmt = result.value();
 *       // Process statement
 *   }
 */
class Parser {
public:
  /**
   * @brief Construct a parser for the given SQL input
   */
  explicit Parser(std::string_view sql);

  /**
   * @brief Parse the SQL input and return the statement AST
   */
  [[nodiscard]] Status parse(std::unique_ptr<Statement> *result);

  /**
   * @brief Get the parse error if parsing failed
   */
  [[nodiscard]] const ParseError &error() const noexcept { return error_; }

private:
  // ─────────────────────────────────────────────────────────────────────────
  // Statement Parsing
  // ─────────────────────────────────────────────────────────────────────────

  std::unique_ptr<Statement> parse_statement();
  std::unique_ptr<SelectStatement> parse_select();
  std::unique_ptr<InsertStatement> parse_insert();
  std::unique_ptr<UpdateStatement> parse_update();
  std::unique_ptr<DeleteStatement> parse_delete();
  std::unique_ptr<CreateTableStatement> parse_create_table();
  std::unique_ptr<DropTableStatement> parse_drop_table();

  // ─────────────────────────────────────────────────────────────────────────
  // Expression Parsing
  // ─────────────────────────────────────────────────────────────────────────

  std::unique_ptr<Expression> parse_expression();
  std::unique_ptr<Expression> parse_or_expression();
  std::unique_ptr<Expression> parse_and_expression();
  std::unique_ptr<Expression> parse_comparison();
  std::unique_ptr<Expression> parse_additive();
  std::unique_ptr<Expression> parse_multiplicative();
  std::unique_ptr<Expression> parse_unary();
  std::unique_ptr<Expression> parse_primary();

  // ─────────────────────────────────────────────────────────────────────────
  // Helpers
  // ─────────────────────────────────────────────────────────────────────────

  // Token navigation
  Token current_token();
  Token peek_token();
  void advance();
  bool check(TokenType type);
  bool match(TokenType type);
  void expect(TokenType type, const std::string &msg);

  // Error handling
  void set_error(const std::string &message);

  // Type parsing
  TypeId parse_data_type(size_t *length_out = nullptr);

  Lexer lexer_;
  Token current_;
  ParseError error_;
  bool has_error_ = false;
};

} // namespace entropy
