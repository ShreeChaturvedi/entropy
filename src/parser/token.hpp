#pragma once

/**
 * @file token.hpp
 * @brief SQL Tokenizer/Lexer for custom SQL parser
 *
 * Implements a lexical analyzer that converts SQL text into tokens.
 * Supports SQL keywords, operators, literals, and identifiers.
 */

#include <cctype>
#include <string>
#include <string_view>
#include <unordered_map>

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Token Types
// ─────────────────────────────────────────────────────────────────────────────

enum class TokenType {
  // Keywords - DDL
  CREATE,
  DROP,
  TABLE,
  INDEX,
  PRIMARY,
  KEY,

  // Keywords - DML
  SELECT,
  INSERT,
  UPDATE,
  DELETE,
  FROM,
  WHERE,
  INTO,
  VALUES,
  SET,

  // Keywords - Logical
  AND,
  OR,
  NOT,
  IS,
  NULL_KEYWORD,
  TRUE_KEYWORD,
  FALSE_KEYWORD,

  // Keywords - Other
  AS,
  ORDER,
  BY,
  ASC,
  DESC,
  LIMIT,
  OFFSET,

  // Data types
  INT,
  INTEGER,
  BIGINT,
  SMALLINT,
  BOOLEAN,
  VARCHAR,
  TEXT,
  FLOAT,
  DOUBLE,

  // Symbols
  LPAREN,    // (
  RPAREN,    // )
  COMMA,     // ,
  SEMICOLON, // ;
  STAR,      // *
  DOT,       // .

  // Comparison operators
  EQ, // =
  NE, // != or <>
  LT, // <
  LE, // <=
  GT, // >
  GE, // >=

  // Arithmetic operators
  PLUS,  // +
  MINUS, // -
  SLASH, // /

  // Literals
  INTEGER_LITERAL,
  FLOAT_LITERAL,
  STRING_LITERAL,

  // Identifier
  IDENTIFIER,

  // Special
  END_OF_FILE,
  INVALID,
};

/**
 * @brief Convert token type to string for debugging
 */
[[nodiscard]] const char *token_type_to_string(TokenType type);

// ─────────────────────────────────────────────────────────────────────────────
// Token
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief A single token from the SQL input
 */
struct Token {
  TokenType type = TokenType::INVALID;
  std::string value; // The actual text of the token
  size_t line = 1;   // Line number (1-indexed)
  size_t column = 1; // Column number (1-indexed)

  Token() = default;
  Token(TokenType t, std::string v, size_t l = 1, size_t c = 1)
      : type(t), value(std::move(v)), line(l), column(c) {}

  [[nodiscard]] bool is(TokenType t) const noexcept { return type == t; }

  [[nodiscard]] bool is_keyword() const noexcept {
    return type >= TokenType::CREATE && type <= TokenType::DOUBLE;
  }

  [[nodiscard]] bool is_literal() const noexcept {
    return type == TokenType::INTEGER_LITERAL ||
           type == TokenType::FLOAT_LITERAL ||
           type == TokenType::STRING_LITERAL;
  }

  [[nodiscard]] bool is_comparison_op() const noexcept {
    return type >= TokenType::EQ && type <= TokenType::GE;
  }

  [[nodiscard]] bool is_eof() const noexcept {
    return type == TokenType::END_OF_FILE;
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// Lexer
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief SQL Lexer - converts SQL text into a stream of tokens
 *
 * Usage:
 *   Lexer lexer("SELECT * FROM users WHERE id = 1");
 *   Token token = lexer.next_token();
 *   while (!token.is_eof()) {
 *       // process token
 *       token = lexer.next_token();
 *   }
 */
class Lexer {
public:
  /**
   * @brief Construct a lexer for the given SQL input
   */
  explicit Lexer(std::string_view sql);

  /**
   * @brief Get the next token and advance
   */
  Token next_token();

  /**
   * @brief Peek at the next token without consuming it
   */
  Token peek_token();

  /**
   * @brief Get current line number
   */
  [[nodiscard]] size_t current_line() const noexcept { return line_; }

  /**
   * @brief Get current column number
   */
  [[nodiscard]] size_t current_column() const noexcept { return column_; }

private:
  // Character navigation
  [[nodiscard]] char current_char() const noexcept;
  [[nodiscard]] char peek_char(size_t offset = 1) const noexcept;
  void advance();
  void skip_whitespace();
  void skip_line_comment();
  void skip_block_comment();

  // Token scanners
  Token scan_identifier_or_keyword();
  Token scan_number();
  Token scan_string();
  Token scan_operator();

  // Helpers
  Token make_token(TokenType type, std::string value);
  Token make_token(TokenType type);

  std::string sql_;
  size_t pos_ = 0;
  size_t line_ = 1;
  size_t column_ = 1;

  // Cached peek token
  bool has_peeked_ = false;
  Token peeked_token_;

  // Keyword lookup table
  static const std::unordered_map<std::string, TokenType> keywords_;
};

} // namespace entropy
