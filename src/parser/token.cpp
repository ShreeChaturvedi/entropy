/**
 * @file token.cpp
 * @brief SQL Lexer implementation
 */

#include "parser/token.hpp"

#include <stdexcept>

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Keyword lookup table
// ─────────────────────────────────────────────────────────────────────────────

const std::unordered_map<std::string, TokenType> Lexer::keywords_ = {
    // DDL
    {"CREATE", TokenType::CREATE},
    {"DROP", TokenType::DROP},
    {"TABLE", TokenType::TABLE},
    {"INDEX", TokenType::INDEX},
    {"PRIMARY", TokenType::PRIMARY},
    {"KEY", TokenType::KEY},

    // DML
    {"SELECT", TokenType::SELECT},
    {"INSERT", TokenType::INSERT},
    {"UPDATE", TokenType::UPDATE},
    {"DELETE", TokenType::DELETE},
    {"FROM", TokenType::FROM},
    {"WHERE", TokenType::WHERE},
    {"INTO", TokenType::INTO},
    {"VALUES", TokenType::VALUES},
    {"SET", TokenType::SET},

    // Logical
    {"AND", TokenType::AND},
    {"OR", TokenType::OR},
    {"NOT", TokenType::NOT},
    {"IS", TokenType::IS},
    {"NULL", TokenType::NULL_KEYWORD},
    {"TRUE", TokenType::TRUE_KEYWORD},
    {"FALSE", TokenType::FALSE_KEYWORD},

    // JOIN
    {"JOIN", TokenType::JOIN},
    {"INNER", TokenType::INNER},
    {"LEFT", TokenType::LEFT},
    {"RIGHT", TokenType::RIGHT},
    {"OUTER", TokenType::OUTER},
    {"CROSS", TokenType::CROSS},
    {"ON", TokenType::ON},

    // Other
    {"AS", TokenType::AS},
    {"ORDER", TokenType::ORDER},
    {"BY", TokenType::BY},
    {"ASC", TokenType::ASC},
    {"DESC", TokenType::DESC},
    {"LIMIT", TokenType::LIMIT},
    {"OFFSET", TokenType::OFFSET},

    // Data types
    {"INT", TokenType::INT},
    {"INTEGER", TokenType::INTEGER},
    {"BIGINT", TokenType::BIGINT},
    {"SMALLINT", TokenType::SMALLINT},
    {"BOOLEAN", TokenType::BOOLEAN},
    {"VARCHAR", TokenType::VARCHAR},
    {"TEXT", TokenType::TEXT},
    {"FLOAT", TokenType::FLOAT},
    {"DOUBLE", TokenType::DOUBLE},
};

const char *token_type_to_string(TokenType type) {
  switch (type) {
  case TokenType::CREATE:
    return "CREATE";
  case TokenType::DROP:
    return "DROP";
  case TokenType::TABLE:
    return "TABLE";
  case TokenType::INDEX:
    return "INDEX";
  case TokenType::PRIMARY:
    return "PRIMARY";
  case TokenType::KEY:
    return "KEY";
  case TokenType::SELECT:
    return "SELECT";
  case TokenType::INSERT:
    return "INSERT";
  case TokenType::UPDATE:
    return "UPDATE";
  case TokenType::DELETE:
    return "DELETE";
  case TokenType::FROM:
    return "FROM";
  case TokenType::WHERE:
    return "WHERE";
  case TokenType::INTO:
    return "INTO";
  case TokenType::VALUES:
    return "VALUES";
  case TokenType::SET:
    return "SET";
  case TokenType::AND:
    return "AND";
  case TokenType::OR:
    return "OR";
  case TokenType::NOT:
    return "NOT";
  case TokenType::IS:
    return "IS";
  case TokenType::NULL_KEYWORD:
    return "NULL";
  case TokenType::TRUE_KEYWORD:
    return "TRUE";
  case TokenType::FALSE_KEYWORD:
    return "FALSE";
  case TokenType::JOIN:
    return "JOIN";
  case TokenType::INNER:
    return "INNER";
  case TokenType::LEFT:
    return "LEFT";
  case TokenType::RIGHT:
    return "RIGHT";
  case TokenType::OUTER:
    return "OUTER";
  case TokenType::CROSS:
    return "CROSS";
  case TokenType::ON:
    return "ON";
  case TokenType::AS:
    return "AS";
  case TokenType::ORDER:
    return "ORDER";
  case TokenType::BY:
    return "BY";
  case TokenType::ASC:
    return "ASC";
  case TokenType::DESC:
    return "DESC";
  case TokenType::LIMIT:
    return "LIMIT";
  case TokenType::OFFSET:
    return "OFFSET";
  case TokenType::INT:
    return "INT";
  case TokenType::INTEGER:
    return "INTEGER";
  case TokenType::BIGINT:
    return "BIGINT";
  case TokenType::SMALLINT:
    return "SMALLINT";
  case TokenType::BOOLEAN:
    return "BOOLEAN";
  case TokenType::VARCHAR:
    return "VARCHAR";
  case TokenType::TEXT:
    return "TEXT";
  case TokenType::FLOAT:
    return "FLOAT";
  case TokenType::DOUBLE:
    return "DOUBLE";
  case TokenType::LPAREN:
    return "LPAREN";
  case TokenType::RPAREN:
    return "RPAREN";
  case TokenType::COMMA:
    return "COMMA";
  case TokenType::SEMICOLON:
    return "SEMICOLON";
  case TokenType::STAR:
    return "STAR";
  case TokenType::DOT:
    return "DOT";
  case TokenType::EQ:
    return "EQ";
  case TokenType::NE:
    return "NE";
  case TokenType::LT:
    return "LT";
  case TokenType::LE:
    return "LE";
  case TokenType::GT:
    return "GT";
  case TokenType::GE:
    return "GE";
  case TokenType::PLUS:
    return "PLUS";
  case TokenType::MINUS:
    return "MINUS";
  case TokenType::SLASH:
    return "SLASH";
  case TokenType::INTEGER_LITERAL:
    return "INTEGER_LITERAL";
  case TokenType::FLOAT_LITERAL:
    return "FLOAT_LITERAL";
  case TokenType::STRING_LITERAL:
    return "STRING_LITERAL";
  case TokenType::IDENTIFIER:
    return "IDENTIFIER";
  case TokenType::END_OF_FILE:
    return "END_OF_FILE";
  case TokenType::INVALID:
    return "INVALID";
  }
  return "UNKNOWN";
}

// ─────────────────────────────────────────────────────────────────────────────
// Lexer Implementation
// ─────────────────────────────────────────────────────────────────────────────

Lexer::Lexer(std::string_view sql) : sql_(sql) {}

Token Lexer::next_token() {
  // Return cached peek if available
  if (has_peeked_) {
    has_peeked_ = false;
    return std::move(peeked_token_);
  }

  skip_whitespace();

  if (pos_ >= sql_.size()) {
    return make_token(TokenType::END_OF_FILE, "");
  }

  char c = current_char();

  // Identifiers and keywords
  if (std::isalpha(c) || c == '_') {
    return scan_identifier_or_keyword();
  }

  // Numbers
  if (std::isdigit(c)) {
    return scan_number();
  }

  // Strings
  if (c == '\'' || c == '"') {
    return scan_string();
  }

  // Operators and symbols
  return scan_operator();
}

Token Lexer::peek_token() {
  if (!has_peeked_) {
    peeked_token_ = next_token();
    has_peeked_ = true;
  }
  return peeked_token_;
}

char Lexer::current_char() const noexcept {
  return pos_ < sql_.size() ? sql_[pos_] : '\0';
}

char Lexer::peek_char(size_t offset) const noexcept {
  size_t idx = pos_ + offset;
  return idx < sql_.size() ? sql_[idx] : '\0';
}

void Lexer::advance() {
  if (pos_ < sql_.size()) {
    if (sql_[pos_] == '\n') {
      line_++;
      column_ = 1;
    } else {
      column_++;
    }
    pos_++;
  }
}

void Lexer::skip_whitespace() {
  while (pos_ < sql_.size()) {
    char c = current_char();
    if (std::isspace(c)) {
      advance();
    } else if (c == '-' && peek_char() == '-') {
      // SQL line comment
      skip_line_comment();
    } else if (c == '/' && peek_char() == '*') {
      // SQL block comment
      skip_block_comment();
    } else {
      break;
    }
  }
}

void Lexer::skip_line_comment() {
  // Skip past --
  advance();
  advance();
  while (pos_ < sql_.size() && current_char() != '\n') {
    advance();
  }
  if (pos_ < sql_.size()) {
    advance(); // Skip the newline
  }
}

void Lexer::skip_block_comment() {
  // Skip past /*
  advance();
  advance();
  while (pos_ < sql_.size()) {
    if (current_char() == '*' && peek_char() == '/') {
      advance();
      advance();
      break;
    }
    advance();
  }
}

Token Lexer::scan_identifier_or_keyword() {
  size_t start_col = column_;
  std::string value;

  while (pos_ < sql_.size()) {
    char c = current_char();
    if (std::isalnum(c) || c == '_') {
      value += c;
      advance();
    } else {
      break;
    }
  }

  // Convert to uppercase for keyword lookup
  std::string upper = value;
  for (char &ch : upper) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }

  // Check if it's a keyword
  auto it = keywords_.find(upper);
  if (it != keywords_.end()) {
    return Token(it->second, value, line_, start_col);
  }

  // It's an identifier
  return Token(TokenType::IDENTIFIER, value, line_, start_col);
}

Token Lexer::scan_number() {
  size_t start_col = column_;
  std::string value;
  bool has_dot = false;

  while (pos_ < sql_.size()) {
    char c = current_char();
    if (std::isdigit(c)) {
      value += c;
      advance();
    } else if (c == '.' && !has_dot && std::isdigit(peek_char())) {
      has_dot = true;
      value += c;
      advance();
    } else {
      break;
    }
  }

  TokenType type =
      has_dot ? TokenType::FLOAT_LITERAL : TokenType::INTEGER_LITERAL;
  return Token(type, value, line_, start_col);
}

Token Lexer::scan_string() {
  size_t start_col = column_;
  char quote = current_char();
  advance(); // Skip opening quote

  std::string value;
  while (pos_ < sql_.size()) {
    char c = current_char();
    if (c == quote) {
      // Check for escaped quote (doubled)
      if (peek_char() == quote) {
        value += quote;
        advance();
        advance();
      } else {
        advance(); // Skip closing quote
        break;
      }
    } else if (c == '\\') {
      // Handle escape sequences
      advance();
      char escaped = current_char();
      switch (escaped) {
      case 'n':
        value += '\n';
        break;
      case 't':
        value += '\t';
        break;
      case 'r':
        value += '\r';
        break;
      case '\\':
        value += '\\';
        break;
      case '\'':
        value += '\'';
        break;
      case '"':
        value += '"';
        break;
      default:
        value += escaped;
        break;
      }
      advance();
    } else {
      value += c;
      advance();
    }
  }

  return Token(TokenType::STRING_LITERAL, value, line_, start_col);
}

Token Lexer::scan_operator() {
  size_t start_col = column_;
  char c = current_char();
  advance();

  switch (c) {
  case '(':
    return Token(TokenType::LPAREN, "(", line_, start_col);
  case ')':
    return Token(TokenType::RPAREN, ")", line_, start_col);
  case ',':
    return Token(TokenType::COMMA, ",", line_, start_col);
  case ';':
    return Token(TokenType::SEMICOLON, ";", line_, start_col);
  case '*':
    return Token(TokenType::STAR, "*", line_, start_col);
  case '.':
    return Token(TokenType::DOT, ".", line_, start_col);
  case '+':
    return Token(TokenType::PLUS, "+", line_, start_col);
  case '-':
    return Token(TokenType::MINUS, "-", line_, start_col);
  case '/':
    return Token(TokenType::SLASH, "/", line_, start_col);

  case '=':
    return Token(TokenType::EQ, "=", line_, start_col);

  case '!':
    if (current_char() == '=') {
      advance();
      return Token(TokenType::NE, "!=", line_, start_col);
    }
    return Token(TokenType::INVALID, "!", line_, start_col);

  case '<':
    if (current_char() == '=') {
      advance();
      return Token(TokenType::LE, "<=", line_, start_col);
    }
    if (current_char() == '>') {
      advance();
      return Token(TokenType::NE, "<>", line_, start_col);
    }
    return Token(TokenType::LT, "<", line_, start_col);

  case '>':
    if (current_char() == '=') {
      advance();
      return Token(TokenType::GE, ">=", line_, start_col);
    }
    return Token(TokenType::GT, ">", line_, start_col);

  default:
    return Token(TokenType::INVALID, std::string(1, c), line_, start_col);
  }
}

Token Lexer::make_token(TokenType type, std::string value) {
  return Token(type, std::move(value), line_, column_);
}

Token Lexer::make_token(TokenType type) {
  return Token(type, "", line_, column_);
}

} // namespace entropy
