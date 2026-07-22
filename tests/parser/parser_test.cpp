/**
 * @file parser_test.cpp
 * @brief Tests for SQL tokenizer, parser, binder, and expression evaluation
 */

#include <gtest/gtest.h>

#include "catalog/catalog.hpp"
#include "parser/binder.hpp"
#include "parser/expression.hpp"
#include "parser/parser.hpp"
#include "parser/statement.hpp"
#include "parser/token.hpp"
#include "storage/buffer_pool.hpp"
#include "test_utils.hpp"

namespace entropy {
namespace {

// ─────────────────────────────────────────────────────────────────────────────
// Lexer Tests
// ─────────────────────────────────────────────────────────────────────────────

class LexerTest : public ::testing::Test {};

TEST_F(LexerTest, TokenizeKeywords) {
  Lexer lexer("SELECT FROM WHERE INSERT INTO VALUES");

  EXPECT_EQ(lexer.next_token().type, TokenType::SELECT);
  EXPECT_EQ(lexer.next_token().type, TokenType::FROM);
  EXPECT_EQ(lexer.next_token().type, TokenType::WHERE);
  EXPECT_EQ(lexer.next_token().type, TokenType::INSERT);
  EXPECT_EQ(lexer.next_token().type, TokenType::INTO);
  EXPECT_EQ(lexer.next_token().type, TokenType::VALUES);
  EXPECT_EQ(lexer.next_token().type, TokenType::END_OF_FILE);
}

TEST_F(LexerTest, TokenizeIdentifiers) {
  Lexer lexer("users user_name _private id123");

  Token t1 = lexer.next_token();
  EXPECT_EQ(t1.type, TokenType::IDENTIFIER);
  EXPECT_EQ(t1.value, "users");

  Token t2 = lexer.next_token();
  EXPECT_EQ(t2.type, TokenType::IDENTIFIER);
  EXPECT_EQ(t2.value, "user_name");

  Token t3 = lexer.next_token();
  EXPECT_EQ(t3.type, TokenType::IDENTIFIER);
  EXPECT_EQ(t3.value, "_private");

  Token t4 = lexer.next_token();
  EXPECT_EQ(t4.type, TokenType::IDENTIFIER);
  EXPECT_EQ(t4.value, "id123");
}

TEST_F(LexerTest, TokenizeNumbers) {
  Lexer lexer("42 3.14 0 -10");

  Token t1 = lexer.next_token();
  EXPECT_EQ(t1.type, TokenType::INTEGER_LITERAL);
  EXPECT_EQ(t1.value, "42");

  Token t2 = lexer.next_token();
  EXPECT_EQ(t2.type, TokenType::FLOAT_LITERAL);
  EXPECT_EQ(t2.value, "3.14");

  Token t3 = lexer.next_token();
  EXPECT_EQ(t3.type, TokenType::INTEGER_LITERAL);
  EXPECT_EQ(t3.value, "0");

  // Minus is a separate token
  EXPECT_EQ(lexer.next_token().type, TokenType::MINUS);
  Token t4 = lexer.next_token();
  EXPECT_EQ(t4.type, TokenType::INTEGER_LITERAL);
  EXPECT_EQ(t4.value, "10");
}

TEST_F(LexerTest, TokenizeStrings) {
  Lexer lexer("'hello' \"world\" 'it''s'");

  Token t1 = lexer.next_token();
  EXPECT_EQ(t1.type, TokenType::STRING_LITERAL);
  EXPECT_EQ(t1.value, "hello");

  Token t2 = lexer.next_token();
  EXPECT_EQ(t2.type, TokenType::STRING_LITERAL);
  EXPECT_EQ(t2.value, "world");

  Token t3 = lexer.next_token();
  EXPECT_EQ(t3.type, TokenType::STRING_LITERAL);
  EXPECT_EQ(t3.value, "it's"); // Escaped quote
}

TEST_F(LexerTest, TokenizeOperators) {
  Lexer lexer("= != <> < <= > >= + - * /");

  EXPECT_EQ(lexer.next_token().type, TokenType::EQ);
  EXPECT_EQ(lexer.next_token().type, TokenType::NE);
  EXPECT_EQ(lexer.next_token().type, TokenType::NE);
  EXPECT_EQ(lexer.next_token().type, TokenType::LT);
  EXPECT_EQ(lexer.next_token().type, TokenType::LE);
  EXPECT_EQ(lexer.next_token().type, TokenType::GT);
  EXPECT_EQ(lexer.next_token().type, TokenType::GE);
  EXPECT_EQ(lexer.next_token().type, TokenType::PLUS);
  EXPECT_EQ(lexer.next_token().type, TokenType::MINUS);
  EXPECT_EQ(lexer.next_token().type, TokenType::STAR);
  EXPECT_EQ(lexer.next_token().type, TokenType::SLASH);
}

TEST_F(LexerTest, TokenizeSymbols) {
  Lexer lexer("( ) , ; .");

  EXPECT_EQ(lexer.next_token().type, TokenType::LPAREN);
  EXPECT_EQ(lexer.next_token().type, TokenType::RPAREN);
  EXPECT_EQ(lexer.next_token().type, TokenType::COMMA);
  EXPECT_EQ(lexer.next_token().type, TokenType::SEMICOLON);
  EXPECT_EQ(lexer.next_token().type, TokenType::DOT);
}

TEST_F(LexerTest, SkipComments) {
  Lexer lexer("SELECT -- this is a comment\n* FROM users");

  EXPECT_EQ(lexer.next_token().type, TokenType::SELECT);
  EXPECT_EQ(lexer.next_token().type, TokenType::STAR);
  EXPECT_EQ(lexer.next_token().type, TokenType::FROM);
}

TEST_F(LexerTest, CompleteSelectStatement) {
  Lexer lexer("SELECT id, name FROM users WHERE age > 21;");

  EXPECT_EQ(lexer.next_token().type, TokenType::SELECT);
  EXPECT_EQ(lexer.next_token().type, TokenType::IDENTIFIER); // id
  EXPECT_EQ(lexer.next_token().type, TokenType::COMMA);
  EXPECT_EQ(lexer.next_token().type, TokenType::IDENTIFIER); // name
  EXPECT_EQ(lexer.next_token().type, TokenType::FROM);
  EXPECT_EQ(lexer.next_token().type, TokenType::IDENTIFIER); // users
  EXPECT_EQ(lexer.next_token().type, TokenType::WHERE);
  EXPECT_EQ(lexer.next_token().type, TokenType::IDENTIFIER); // age
  EXPECT_EQ(lexer.next_token().type, TokenType::GT);
  EXPECT_EQ(lexer.next_token().type, TokenType::INTEGER_LITERAL); // 21
  EXPECT_EQ(lexer.next_token().type, TokenType::SEMICOLON);
  EXPECT_EQ(lexer.next_token().type, TokenType::END_OF_FILE);
}

// ─────────────────────────────────────────────────────────────────────────────
// Parser Tests
// ─────────────────────────────────────────────────────────────────────────────

class ParserTest : public ::testing::Test {};

TEST_F(ParserTest, ParseSelectStar) {
  Parser parser("SELECT * FROM users");
  std::unique_ptr<Statement> stmt;

  EXPECT_TRUE(parser.parse(&stmt).ok());
  ASSERT_NE(stmt, nullptr);
  EXPECT_EQ(stmt->type(), StatementType::SELECT);

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  ASSERT_NE(select, nullptr);
  ASSERT_EQ(select->columns.size(), 1);
  EXPECT_TRUE(select->columns[0].is_star);
  EXPECT_EQ(select->table.table_name, "users");
  EXPECT_EQ(select->where_clause, nullptr);
}

TEST_F(ParserTest, ParseSelectColumns) {
  Parser parser("SELECT id, name, age FROM employees");
  std::unique_ptr<Statement> stmt;

  EXPECT_TRUE(parser.parse(&stmt).ok());
  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  ASSERT_NE(select, nullptr);

  ASSERT_EQ(select->columns.size(), 3);
  EXPECT_EQ(select->columns[0].column_name, "id");
  EXPECT_EQ(select->columns[1].column_name, "name");
  EXPECT_EQ(select->columns[2].column_name, "age");
  EXPECT_EQ(select->table.table_name, "employees");
}

TEST_F(ParserTest, ParseSelectWithWhere) {
  Parser parser("SELECT * FROM users WHERE id = 1");
  std::unique_ptr<Statement> stmt;

  EXPECT_TRUE(parser.parse(&stmt).ok());
  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  ASSERT_NE(select, nullptr);

  EXPECT_NE(select->where_clause, nullptr);
  EXPECT_EQ(select->where_clause->expr_type(), ExpressionType::COMPARISON);
}

TEST_F(ParserTest, ParseSelectWithOrderBy) {
  Parser parser("SELECT * FROM users ORDER BY name ASC, age DESC");
  std::unique_ptr<Statement> stmt;

  EXPECT_TRUE(parser.parse(&stmt).ok());
  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  ASSERT_NE(select, nullptr);

  ASSERT_EQ(select->order_by.size(), 2);
  EXPECT_EQ(select->order_by[0].column_name, "name");
  EXPECT_TRUE(select->order_by[0].ascending);
  EXPECT_EQ(select->order_by[1].column_name, "age");
  EXPECT_FALSE(select->order_by[1].ascending);
}

TEST_F(ParserTest, ParseSelectWithLimit) {
  Parser parser("SELECT * FROM users LIMIT 10 OFFSET 5");
  std::unique_ptr<Statement> stmt;

  EXPECT_TRUE(parser.parse(&stmt).ok());
  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  ASSERT_NE(select, nullptr);

  EXPECT_TRUE(select->limit.has_value());
  EXPECT_EQ(select->limit.value(), 10);
  EXPECT_TRUE(select->offset.has_value());
  EXPECT_EQ(select->offset.value(), 5);
}

TEST_F(ParserTest, ParseInsert) {
  Parser parser("INSERT INTO users (id, name) VALUES (1, 'Alice')");
  std::unique_ptr<Statement> stmt;

  EXPECT_TRUE(parser.parse(&stmt).ok());
  EXPECT_EQ(stmt->type(), StatementType::INSERT);

  auto *insert = dynamic_cast<InsertStatement *>(stmt.get());
  ASSERT_NE(insert, nullptr);

  EXPECT_EQ(insert->table_name, "users");
  ASSERT_EQ(insert->columns.size(), 2);
  EXPECT_EQ(insert->columns[0], "id");
  EXPECT_EQ(insert->columns[1], "name");

  ASSERT_EQ(insert->values.size(), 1);
  ASSERT_EQ(insert->values[0].size(), 2);
  EXPECT_EQ(std::get<int64_t>(insert->values[0][0]), 1);
  EXPECT_EQ(std::get<std::string>(insert->values[0][1]), "Alice");
}

TEST_F(ParserTest, ParseInsertMultipleRows) {
  Parser parser("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");
  std::unique_ptr<Statement> stmt;

  EXPECT_TRUE(parser.parse(&stmt).ok());
  auto *insert = dynamic_cast<InsertStatement *>(stmt.get());
  ASSERT_NE(insert, nullptr);

  ASSERT_EQ(insert->values.size(), 2);
  EXPECT_EQ(std::get<int64_t>(insert->values[0][0]), 1);
  EXPECT_EQ(std::get<int64_t>(insert->values[1][0]), 2);
}

TEST_F(ParserTest, ParseUpdate) {
  Parser parser("UPDATE users SET name = 'Bob' WHERE id = 1");
  std::unique_ptr<Statement> stmt;

  EXPECT_TRUE(parser.parse(&stmt).ok());
  EXPECT_EQ(stmt->type(), StatementType::UPDATE);

  auto *update = dynamic_cast<UpdateStatement *>(stmt.get());
  ASSERT_NE(update, nullptr);

  EXPECT_EQ(update->table_name, "users");
  ASSERT_EQ(update->set_clauses.size(), 1);
  EXPECT_EQ(update->set_clauses[0].column_name, "name");
  EXPECT_NE(update->where_clause, nullptr);
}

TEST_F(ParserTest, ParseDelete) {
  Parser parser("DELETE FROM users WHERE id = 1");
  std::unique_ptr<Statement> stmt;

  EXPECT_TRUE(parser.parse(&stmt).ok());
  EXPECT_EQ(stmt->type(), StatementType::DELETE_STMT);

  auto *del = dynamic_cast<DeleteStatement *>(stmt.get());
  ASSERT_NE(del, nullptr);

  EXPECT_EQ(del->table_name, "users");
  EXPECT_NE(del->where_clause, nullptr);
}

TEST_F(ParserTest, ParseCreateTable) {
  Parser parser("CREATE TABLE users (id INTEGER PRIMARY KEY, name "
                "VARCHAR(100), active BOOLEAN)");
  std::unique_ptr<Statement> stmt;

  EXPECT_TRUE(parser.parse(&stmt).ok());
  EXPECT_EQ(stmt->type(), StatementType::CREATE_TABLE);

  auto *create = dynamic_cast<CreateTableStatement *>(stmt.get());
  ASSERT_NE(create, nullptr);

  EXPECT_EQ(create->table_name, "users");
  ASSERT_EQ(create->columns.size(), 3);

  EXPECT_EQ(create->columns[0].name, "id");
  EXPECT_EQ(create->columns[0].type, TypeId::INTEGER);
  EXPECT_TRUE(create->columns[0].is_primary_key);

  EXPECT_EQ(create->columns[1].name, "name");
  EXPECT_EQ(create->columns[1].type, TypeId::VARCHAR);
  EXPECT_EQ(create->columns[1].length, 100);

  EXPECT_EQ(create->columns[2].name, "active");
  EXPECT_EQ(create->columns[2].type, TypeId::BOOLEAN);
}

TEST_F(ParserTest, ParseDropTable) {
  Parser parser("DROP TABLE users");
  std::unique_ptr<Statement> stmt;

  EXPECT_TRUE(parser.parse(&stmt).ok());
  EXPECT_EQ(stmt->type(), StatementType::DROP_TABLE);

  auto *drop = dynamic_cast<DropTableStatement *>(stmt.get());
  ASSERT_NE(drop, nullptr);
  EXPECT_EQ(drop->table_name, "users");
}

TEST_F(ParserTest, ParseComplexWhere) {
  Parser parser("SELECT * FROM t WHERE a > 1 AND b < 2 OR c = 3");
  std::unique_ptr<Statement> stmt;

  EXPECT_TRUE(parser.parse(&stmt).ok());
  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  ASSERT_NE(select, nullptr);

  // Should parse as: (a > 1 AND b < 2) OR c = 3
  EXPECT_NE(select->where_clause, nullptr);
  EXPECT_EQ(select->where_clause->expr_type(), ExpressionType::LOGICAL);
}

TEST_F(ParserTest, ParseError) {
  Parser parser("SELECT FROM"); // Missing column list or *
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  EXPECT_FALSE(status.ok());
}

// ─────────────────────────────────────────────────────────────────────────────
// Numeric literal overflow / malformed handling (issue #28)
//
// Overflowing or malformed numeric literals must be reported through the
// Status-based error path, never crash the process with an uncaught
// std::out_of_range / std::invalid_argument.
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(ParserTest, LimitLiteralOverflow) {
  Parser parser("SELECT * FROM t LIMIT 99999999999999999999999");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  EXPECT_FALSE(status.ok());
}

TEST_F(ParserTest, OffsetLiteralOverflow) {
  Parser parser("SELECT * FROM t LIMIT 10 OFFSET 99999999999999999999999");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  EXPECT_FALSE(status.ok());
}

TEST_F(ParserTest, WhereIntLiteralOverflow) {
  Parser parser("SELECT * FROM t WHERE id = 99999999999999999999999");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  EXPECT_FALSE(status.ok());
}

TEST_F(ParserTest, WhereNegativeIntLiteralOverflow) {
  Parser parser("SELECT * FROM t WHERE id = -99999999999999999999999");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  EXPECT_FALSE(status.ok());
}

TEST_F(ParserTest, WhereFloatLiteralOverflow) {
  // A float literal far beyond the range of double (~1.8e308). The lexer has
  // no exponent notation, so the magnitude comes from the digit count.
  std::string big_float = std::string(400, '9') + ".0";
  Parser parser("SELECT * FROM t WHERE x = " + big_float);
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  EXPECT_FALSE(status.ok());
}

TEST_F(ParserTest, InsertIntLiteralOverflow) {
  Parser parser("INSERT INTO t VALUES (99999999999999999999999)");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  EXPECT_FALSE(status.ok());
}

TEST_F(ParserTest, InsertFloatLiteralOverflow) {
  std::string big_float = std::string(400, '9') + ".0";
  Parser parser("INSERT INTO t VALUES (" + big_float + ")");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  EXPECT_FALSE(status.ok());
}

TEST_F(ParserTest, VarcharLengthOverflow) {
  Parser parser("CREATE TABLE t (a VARCHAR(99999999999999999999999))");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  EXPECT_FALSE(status.ok());
}

// A valid boundary literal must still parse cleanly.
TEST_F(ParserTest, MaxInt64LiteralParses) {
  Parser parser("SELECT * FROM t WHERE id = 9223372036854775807");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  EXPECT_TRUE(status.ok());
}

// ─────────────────────────────────────────────────────────────────────────────
// Expression Evaluation Tests
// ─────────────────────────────────────────────────────────────────────────────

class ExpressionTest : public ::testing::Test {
protected:
  // Dummy tuple and schema for evaluation
  Schema schema_;
  Tuple tuple_;
};

TEST_F(ExpressionTest, ConstantExpression) {
  auto expr = std::make_unique<ConstantExpression>(
      TupleValue(static_cast<int64_t>(42)));

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_bigint());
  EXPECT_EQ(result.as_bigint(), 42);
}

TEST_F(ExpressionTest, BinaryAddition) {
  auto left = std::make_unique<ConstantExpression>(
      TupleValue(static_cast<int64_t>(10)));
  auto right =
      std::make_unique<ConstantExpression>(TupleValue(static_cast<int64_t>(5)));

  auto expr = std::make_unique<BinaryOpExpression>(
      BinaryOpType::ADD, std::move(left), std::move(right));

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_bigint());
  EXPECT_EQ(result.as_bigint(), 15);
}

TEST_F(ExpressionTest, Comparison) {
  auto left = std::make_unique<ConstantExpression>(
      TupleValue(static_cast<int64_t>(10)));
  auto right =
      std::make_unique<ConstantExpression>(TupleValue(static_cast<int64_t>(5)));

  auto expr = std::make_unique<ComparisonExpression>(
      ComparisonType::GREATER_THAN, std::move(left), std::move(right));

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_bool());
  EXPECT_TRUE(result.as_bool());
}

TEST_F(ExpressionTest, LogicalAnd) {
  auto left = std::make_unique<ConstantExpression>(TupleValue(true));
  auto right = std::make_unique<ConstantExpression>(TupleValue(false));

  auto expr = std::make_unique<LogicalExpression>(
      LogicalOpType::AND, std::move(left), std::move(right));

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_bool());
  EXPECT_FALSE(result.as_bool());
}

TEST_F(ExpressionTest, LogicalNot) {
  auto operand = std::make_unique<ConstantExpression>(TupleValue(false));

  auto expr = std::make_unique<LogicalExpression>(LogicalOpType::NOT,
                                                  std::move(operand));

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_bool());
  EXPECT_TRUE(result.as_bool());
}

TEST_F(ExpressionTest, StringComparison) {
  auto left =
      std::make_unique<ConstantExpression>(TupleValue(std::string("abc")));
  auto right =
      std::make_unique<ConstantExpression>(TupleValue(std::string("abc")));

  auto expr = std::make_unique<ComparisonExpression>(
      ComparisonType::EQUAL, std::move(left), std::move(right));

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_bool());
  EXPECT_TRUE(result.as_bool());
}

// Incompatible operand types have no defined ordering. Under SQL three-valued
// logic the comparison must evaluate to NULL (not-true), never silently match.
TEST_F(ExpressionTest, IncompatibleTypeEqualIsNull) {
  auto left =
      std::make_unique<ConstantExpression>(TupleValue(static_cast<int64_t>(5)));
  auto right =
      std::make_unique<ConstantExpression>(TupleValue(std::string("abc")));

  auto expr = std::make_unique<ComparisonExpression>(
      ComparisonType::EQUAL, std::move(left), std::move(right));

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_null());
}

TEST_F(ExpressionTest, IncompatibleTypeEqualIsNullStringVsInt) {
  auto left =
      std::make_unique<ConstantExpression>(TupleValue(std::string("abc")));
  auto right =
      std::make_unique<ConstantExpression>(TupleValue(static_cast<int64_t>(5)));

  auto expr = std::make_unique<ComparisonExpression>(
      ComparisonType::EQUAL, std::move(left), std::move(right));

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_null());
}

TEST_F(ExpressionTest, IncompatibleTypeBoolVsIntIsNull) {
  auto left = std::make_unique<ConstantExpression>(TupleValue(true));
  auto right =
      std::make_unique<ConstantExpression>(TupleValue(static_cast<int64_t>(1)));

  auto expr = std::make_unique<ComparisonExpression>(
      ComparisonType::EQUAL, std::move(left), std::move(right));

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_null());
}

TEST_F(ExpressionTest, IncompatibleTypeLessEqualIsNull) {
  auto left =
      std::make_unique<ConstantExpression>(TupleValue(static_cast<int64_t>(5)));
  auto right =
      std::make_unique<ConstantExpression>(TupleValue(std::string("abc")));

  auto expr = std::make_unique<ComparisonExpression>(
      ComparisonType::LESS_EQUAL, std::move(left), std::move(right));

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_null());
}

TEST_F(ExpressionTest, IncompatibleTypeGreaterEqualIsNull) {
  auto left =
      std::make_unique<ConstantExpression>(TupleValue(static_cast<int64_t>(5)));
  auto right =
      std::make_unique<ConstantExpression>(TupleValue(std::string("abc")));

  auto expr = std::make_unique<ComparisonExpression>(
      ComparisonType::GREATER_EQUAL, std::move(left), std::move(right));

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_null());
}

// int/float mixing stays a valid numeric comparison and must NOT become NULL.
TEST_F(ExpressionTest, IntFloatComparisonStillValid) {
  auto left =
      std::make_unique<ConstantExpression>(TupleValue(static_cast<int64_t>(3)));
  auto right = std::make_unique<ConstantExpression>(TupleValue(3.0));

  auto expr = std::make_unique<ComparisonExpression>(
      ComparisonType::EQUAL, std::move(left), std::move(right));

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_bool());
  EXPECT_TRUE(result.as_bool());
}

// ─────────────────────────────────────────────────────────────────────────────
// Binder Tests
// ─────────────────────────────────────────────────────────────────────────────

} // namespace

namespace {

class BinderTest : public ::testing::Test {
protected:
  void SetUp() override {
    temp_file_ = std::make_unique<test::TempFile>("binder_test_");
    disk_manager_ = std::make_shared<FileDiskManager>(temp_file_->string());
    buffer_pool_ = std::make_shared<BufferPoolManager>(10, disk_manager_);
    catalog_ = std::make_unique<Catalog>(buffer_pool_);
    binder_ = std::make_unique<Binder>(catalog_.get());

    // Create a test table
    Schema schema({
        Column("id", TypeId::INTEGER),
        Column("name", TypeId::VARCHAR, 100),
        Column("age", TypeId::INTEGER),
    });
    ASSERT_TRUE(catalog_->create_table("users", schema).ok());
  }

  void TearDown() override {
    binder_.reset();
    catalog_.reset();
    buffer_pool_.reset();
    disk_manager_.reset();
    temp_file_.reset();
  }

  std::unique_ptr<test::TempFile> temp_file_;
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<Binder> binder_;
};

TEST_F(BinderTest, BindSelectStar) {
  Parser parser("SELECT * FROM users");
  std::unique_ptr<Statement> stmt;
  ASSERT_TRUE(parser.parse(&stmt).ok());

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  ASSERT_NE(select, nullptr);

  BoundSelectContext context;
  auto status = binder_->bind_select(select, &context);
  EXPECT_TRUE(status.ok()) << status.to_string();

  EXPECT_NE(context.table_info, nullptr);
  EXPECT_EQ(context.table_info->name, "users");
  EXPECT_TRUE(context.select_all);
  EXPECT_EQ(context.column_indices.size(), 3); // id, name, age
}

TEST_F(BinderTest, BindSelectColumns) {
  Parser parser("SELECT id, age FROM users");
  std::unique_ptr<Statement> stmt;
  ASSERT_TRUE(parser.parse(&stmt).ok());

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  BoundSelectContext context;
  auto status = binder_->bind_select(select, &context);
  EXPECT_TRUE(status.ok()) << status.to_string();

  EXPECT_FALSE(context.select_all);
  ASSERT_EQ(context.column_indices.size(), 2);
  EXPECT_EQ(context.column_indices[0], 0); // id is index 0
  EXPECT_EQ(context.column_indices[1], 2); // age is index 2
}

TEST_F(BinderTest, BindSelectWithWhere) {
  Parser parser("SELECT * FROM users WHERE id = 1");
  std::unique_ptr<Statement> stmt;
  ASSERT_TRUE(parser.parse(&stmt).ok());

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  BoundSelectContext context;
  auto status = binder_->bind_select(select, &context);
  EXPECT_TRUE(status.ok()) << status.to_string();

  EXPECT_NE(context.predicate, nullptr);
}

TEST_F(BinderTest, BindSelectNonexistentTable) {
  Parser parser("SELECT * FROM nonexistent");
  std::unique_ptr<Statement> stmt;
  ASSERT_TRUE(parser.parse(&stmt).ok());

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  BoundSelectContext context;
  auto status = binder_->bind_select(select, &context);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), StatusCode::kNotFound);
}

TEST_F(BinderTest, BindSelectNonexistentColumn) {
  Parser parser("SELECT nonexistent FROM users");
  std::unique_ptr<Statement> stmt;
  ASSERT_TRUE(parser.parse(&stmt).ok());

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  BoundSelectContext context;
  auto status = binder_->bind_select(select, &context);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), StatusCode::kNotFound);
}

TEST_F(BinderTest, BindInsert) {
  Parser parser("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)");
  std::unique_ptr<Statement> stmt;
  ASSERT_TRUE(parser.parse(&stmt).ok());

  auto *insert = dynamic_cast<InsertStatement *>(stmt.get());
  ASSERT_NE(insert, nullptr);

  BoundInsertContext context;
  auto status = binder_->bind_insert(insert, &context);
  EXPECT_TRUE(status.ok()) << status.to_string();

  EXPECT_NE(context.table_info, nullptr);
  ASSERT_EQ(context.column_indices.size(), 3);
  EXPECT_EQ(context.column_indices[0], 0); // id
  EXPECT_EQ(context.column_indices[1], 1); // name
  EXPECT_EQ(context.column_indices[2], 2); // age
}

TEST_F(BinderTest, BindInsertMismatchedValues) {
  Parser parser("INSERT INTO users (id, name) VALUES (1)");
  std::unique_ptr<Statement> stmt;
  ASSERT_TRUE(parser.parse(&stmt).ok());

  auto *insert = dynamic_cast<InsertStatement *>(stmt.get());
  BoundInsertContext context;
  auto status = binder_->bind_insert(insert, &context);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
}

TEST_F(BinderTest, BindDelete) {
  Parser parser("DELETE FROM users WHERE id = 1");
  std::unique_ptr<Statement> stmt;
  ASSERT_TRUE(parser.parse(&stmt).ok());

  auto *del = dynamic_cast<DeleteStatement *>(stmt.get());
  ASSERT_NE(del, nullptr);

  BoundDeleteContext context;
  auto status = binder_->bind_delete(del, &context);
  EXPECT_TRUE(status.ok()) << status.to_string();

  EXPECT_NE(context.table_info, nullptr);
  EXPECT_NE(context.predicate, nullptr);
}

// Regression tests for #29: the binder must descend into the children of
// AND/OR/NOT so that unknown columns under a logical operator are rejected.
TEST_F(BinderTest, BindSelectUnknownColumnUnderAnd) {
  Parser parser("SELECT * FROM users WHERE bogus_col = 1 AND age = 2");
  std::unique_ptr<Statement> stmt;
  ASSERT_TRUE(parser.parse(&stmt).ok());

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  BoundSelectContext context;
  auto status = binder_->bind_select(select, &context);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), StatusCode::kNotFound);
}

TEST_F(BinderTest, BindSelectUnknownColumnUnderOr) {
  Parser parser("SELECT * FROM users WHERE age = 1 OR bogus_col = 2");
  std::unique_ptr<Statement> stmt;
  ASSERT_TRUE(parser.parse(&stmt).ok());

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  BoundSelectContext context;
  auto status = binder_->bind_select(select, &context);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), StatusCode::kNotFound);
}

TEST_F(BinderTest, BindSelectUnknownColumnUnderNot) {
  Parser parser("SELECT * FROM users WHERE NOT (bogus_col = 1)");
  std::unique_ptr<Statement> stmt;
  ASSERT_TRUE(parser.parse(&stmt).ok());

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  BoundSelectContext context;
  auto status = binder_->bind_select(select, &context);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), StatusCode::kNotFound);
}

TEST_F(BinderTest, BindSelectValidColumnsUnderLogical) {
  Parser parser("SELECT * FROM users WHERE age = 1 AND id = 2");
  std::unique_ptr<Statement> stmt;
  ASSERT_TRUE(parser.parse(&stmt).ok());

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  BoundSelectContext context;
  auto status = binder_->bind_select(select, &context);
  EXPECT_TRUE(status.ok()) << status.to_string();
  ASSERT_NE(context.predicate, nullptr);

  // Column indices under the logical operator must be resolved by the binder.
  auto *logical = dynamic_cast<LogicalExpression *>(context.predicate.get());
  ASSERT_NE(logical, nullptr);
  EXPECT_EQ(logical->op(), LogicalOpType::AND);

  auto *lhs = dynamic_cast<const ComparisonExpression *>(logical->left());
  ASSERT_NE(lhs, nullptr);
  auto *lhs_col = dynamic_cast<const ColumnRefExpression *>(lhs->left());
  ASSERT_NE(lhs_col, nullptr);
  EXPECT_EQ(lhs_col->column_index(), 2u); // age is index 2

  auto *rhs = dynamic_cast<const ComparisonExpression *>(logical->right());
  ASSERT_NE(rhs, nullptr);
  auto *rhs_col = dynamic_cast<const ColumnRefExpression *>(rhs->left());
  ASSERT_NE(rhs_col, nullptr);
  EXPECT_EQ(rhs_col->column_index(), 0u); // id is index 0
}

// ─────────────────────────────────────────────────────────────────────────────
// Trailing tokens / GROUP BY / IS NULL (issue #32)
//
// Silent wrong SQL is worse than errors: unconsumed tokens after a statement
// must fail, GROUP BY must not be swallowed as a table alias, and IS [NOT]
// NULL must produce a real expression node.
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(ParserTest, TrailingJunkAfterSelectIsError) {
  Parser parser("SELECT * FROM users WHERE id = 1 EXTRA");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  EXPECT_FALSE(status.ok());
}

TEST_F(ParserTest, TrailingSecondStatementIsError) {
  Parser parser("SELECT * FROM users; DROP TABLE users;");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  EXPECT_FALSE(status.ok());
}

TEST_F(ParserTest, GroupByIsNotTableAlias) {
  Parser parser("SELECT * FROM users GROUP BY id");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  ASSERT_TRUE(status.ok()) << status.to_string();

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  ASSERT_NE(select, nullptr);
  EXPECT_TRUE(select->table.alias.empty());
  ASSERT_EQ(select->group_by.size(), 1u);
  EXPECT_EQ(select->group_by[0], "id");
}

TEST_F(ParserTest, GroupByMultipleColumns) {
  Parser parser("SELECT * FROM users GROUP BY id, name");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  ASSERT_TRUE(status.ok()) << status.to_string();

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  ASSERT_NE(select, nullptr);
  ASSERT_EQ(select->group_by.size(), 2u);
  EXPECT_EQ(select->group_by[0], "id");
  EXPECT_EQ(select->group_by[1], "name");
}

TEST_F(ParserTest, ParseIsNull) {
  Parser parser("SELECT * FROM users WHERE name IS NULL");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  ASSERT_TRUE(status.ok()) << status.to_string();

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  ASSERT_NE(select, nullptr);
  ASSERT_NE(select->where_clause, nullptr);
  EXPECT_EQ(select->where_clause->expr_type(), ExpressionType::IS_NULL);

  auto *is_null = dynamic_cast<IsNullExpression *>(select->where_clause.get());
  ASSERT_NE(is_null, nullptr);
  EXPECT_FALSE(is_null->negated());
}

TEST_F(ParserTest, ParseIsNotNull) {
  Parser parser("SELECT * FROM users WHERE name IS NOT NULL");
  std::unique_ptr<Statement> stmt;

  Status status = parser.parse(&stmt);
  ASSERT_TRUE(status.ok()) << status.to_string();

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  ASSERT_NE(select, nullptr);
  ASSERT_NE(select->where_clause, nullptr);
  EXPECT_EQ(select->where_clause->expr_type(), ExpressionType::IS_NULL);

  auto *is_null = dynamic_cast<IsNullExpression *>(select->where_clause.get());
  ASSERT_NE(is_null, nullptr);
  EXPECT_TRUE(is_null->negated());
}

TEST_F(ExpressionTest, IsNullOnNullValue) {
  auto operand = std::make_unique<ConstantExpression>(TupleValue::null());
  auto expr = std::make_unique<IsNullExpression>(std::move(operand), false);

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_bool());
  EXPECT_TRUE(result.as_bool());
}

TEST_F(ExpressionTest, IsNullOnNonNullValue) {
  auto operand = std::make_unique<ConstantExpression>(
      TupleValue(static_cast<int64_t>(1)));
  auto expr = std::make_unique<IsNullExpression>(std::move(operand), false);

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_bool());
  EXPECT_FALSE(result.as_bool());
}

TEST_F(ExpressionTest, IsNotNullOnNullValue) {
  auto operand = std::make_unique<ConstantExpression>(TupleValue::null());
  auto expr = std::make_unique<IsNullExpression>(std::move(operand), true);

  TupleValue result = expr->evaluate(tuple_, schema_);
  EXPECT_TRUE(result.is_bool());
  EXPECT_FALSE(result.as_bool());
}

TEST_F(BinderTest, BindIsNullUnknownColumn) {
  Parser parser("SELECT * FROM users WHERE bogus_col IS NULL");
  std::unique_ptr<Statement> stmt;
  ASSERT_TRUE(parser.parse(&stmt).ok());

  auto *select = dynamic_cast<SelectStatement *>(stmt.get());
  BoundSelectContext context;
  auto status = binder_->bind_select(select, &context);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), StatusCode::kNotFound);
}

// ─────────────────────────────────────────────────────────────────────────────
// SQL surface gaps and lax lexing (issue #35)
//
// - DECIMAL/TIMESTAMP/DATE/CHAR must be declarable in CREATE TABLE.
// - CREATE INDEX / DROP INDEX must parse into their own statement nodes.
// - Unterminated string literals and block comments must be lex errors.
// - Duplicate column names in CREATE TABLE / INSERT must be rejected.
// ─────────────────────────────────────────────────────────────────────────────

// -- Lexer: malformed input must error, not be silently accepted -------------

TEST_F(LexerTest, UnterminatedStringIsInvalidToken) {
  Lexer lexer("'unterminated");
  EXPECT_EQ(lexer.next_token().type, TokenType::INVALID);
}

TEST_F(LexerTest, UnterminatedStringWithEscapeIsInvalidToken) {
  // A trailing backslash must not let the scanner consume the closing state and
  // fall off the end silently.
  Lexer lexer("'abc\\");
  EXPECT_EQ(lexer.next_token().type, TokenType::INVALID);
}

TEST_F(LexerTest, UnterminatedBlockCommentIsInvalidToken) {
  Lexer lexer("SELECT /* oops");
  EXPECT_EQ(lexer.next_token().type, TokenType::SELECT);
  EXPECT_EQ(lexer.next_token().type, TokenType::INVALID);
}

// Guard: a properly-closed block comment must still be skipped cleanly.
TEST_F(LexerTest, SkipClosedBlockComment) {
  Lexer lexer("SELECT /* comment */ *");
  EXPECT_EQ(lexer.next_token().type, TokenType::SELECT);
  EXPECT_EQ(lexer.next_token().type, TokenType::STAR);
  EXPECT_EQ(lexer.next_token().type, TokenType::END_OF_FILE);
}

TEST_F(ParserTest, UnterminatedStringIsParseError) {
  Parser parser("SELECT * FROM users WHERE name = 'unterminated");
  std::unique_ptr<Statement> stmt;
  EXPECT_FALSE(parser.parse(&stmt).ok());
}

TEST_F(ParserTest, UnterminatedBlockCommentIsParseError) {
  Parser parser("SELECT * FROM users /* oops");
  std::unique_ptr<Statement> stmt;
  EXPECT_FALSE(parser.parse(&stmt).ok());
}

// -- Lexer/parser: extended column types -------------------------------------

TEST_F(LexerTest, TokenizeExtendedTypeKeywords) {
  Lexer lexer("DECIMAL TIMESTAMP DATE CHAR");
  EXPECT_EQ(lexer.next_token().type, TokenType::DECIMAL);
  EXPECT_EQ(lexer.next_token().type, TokenType::TIMESTAMP);
  EXPECT_EQ(lexer.next_token().type, TokenType::DATE);
  EXPECT_EQ(lexer.next_token().type, TokenType::CHAR);
}

TEST_F(ParserTest, ParseCreateTableExtendedTypes) {
  Parser parser("CREATE TABLE t (a DECIMAL, b DECIMAL(10, 2), c TIMESTAMP, "
                "d DATE, e CHAR(8))");
  std::unique_ptr<Statement> stmt;
  Status status = parser.parse(&stmt);
  ASSERT_TRUE(status.ok()) << status.to_string();

  auto *create = dynamic_cast<CreateTableStatement *>(stmt.get());
  ASSERT_NE(create, nullptr);
  ASSERT_EQ(create->columns.size(), 5u);
  EXPECT_EQ(create->columns[0].type, TypeId::DECIMAL);
  EXPECT_EQ(create->columns[1].type, TypeId::DECIMAL);
  EXPECT_EQ(create->columns[2].type, TypeId::TIMESTAMP);
  EXPECT_EQ(create->columns[3].type, TypeId::TIMESTAMP); // DATE bridges to it
  EXPECT_EQ(create->columns[4].type, TypeId::VARCHAR);   // CHAR bridges to it
  EXPECT_EQ(create->columns[4].length, 8u);
}

TEST_F(ParserTest, DecimalPrecisionOverflowIsError) {
  Parser parser("CREATE TABLE t (a DECIMAL(99999999999999999999999))");
  std::unique_ptr<Statement> stmt;
  EXPECT_FALSE(parser.parse(&stmt).ok());
}

TEST_F(ParserTest, DecimalMissingPrecisionIsError) {
  Parser parser("CREATE TABLE t (a DECIMAL())");
  std::unique_ptr<Statement> stmt;
  EXPECT_FALSE(parser.parse(&stmt).ok());
}

// -- Parser: CREATE INDEX / DROP INDEX ---------------------------------------

TEST_F(ParserTest, ParseCreateIndex) {
  Parser parser("CREATE INDEX idx_name ON users (name)");
  std::unique_ptr<Statement> stmt;
  Status status = parser.parse(&stmt);
  ASSERT_TRUE(status.ok()) << status.to_string();
  EXPECT_EQ(stmt->type(), StatementType::CREATE_INDEX);

  auto *idx = dynamic_cast<CreateIndexStatement *>(stmt.get());
  ASSERT_NE(idx, nullptr);
  EXPECT_EQ(idx->index_name, "idx_name");
  EXPECT_EQ(idx->table_name, "users");
  ASSERT_EQ(idx->columns.size(), 1u);
  EXPECT_EQ(idx->columns[0], "name");
}

TEST_F(ParserTest, ParseCreateIndexMultiColumn) {
  Parser parser("CREATE INDEX idx ON t (a, b, c)");
  std::unique_ptr<Statement> stmt;
  Status status = parser.parse(&stmt);
  ASSERT_TRUE(status.ok()) << status.to_string();

  auto *idx = dynamic_cast<CreateIndexStatement *>(stmt.get());
  ASSERT_NE(idx, nullptr);
  ASSERT_EQ(idx->columns.size(), 3u);
  EXPECT_EQ(idx->columns[0], "a");
  EXPECT_EQ(idx->columns[1], "b");
  EXPECT_EQ(idx->columns[2], "c");
}

TEST_F(ParserTest, ParseCreateIndexMissingOnIsError) {
  Parser parser("CREATE INDEX idx users (name)");
  std::unique_ptr<Statement> stmt;
  EXPECT_FALSE(parser.parse(&stmt).ok());
}

TEST_F(ParserTest, ParseDropIndex) {
  Parser parser("DROP INDEX idx_name");
  std::unique_ptr<Statement> stmt;
  Status status = parser.parse(&stmt);
  ASSERT_TRUE(status.ok()) << status.to_string();
  EXPECT_EQ(stmt->type(), StatementType::DROP_INDEX);

  auto *idx = dynamic_cast<DropIndexStatement *>(stmt.get());
  ASSERT_NE(idx, nullptr);
  EXPECT_EQ(idx->index_name, "idx_name");
  EXPECT_TRUE(idx->table_name.empty());
}

TEST_F(ParserTest, ParseDropIndexOnTable) {
  Parser parser("DROP INDEX idx_name ON users");
  std::unique_ptr<Statement> stmt;
  Status status = parser.parse(&stmt);
  ASSERT_TRUE(status.ok()) << status.to_string();

  auto *idx = dynamic_cast<DropIndexStatement *>(stmt.get());
  ASSERT_NE(idx, nullptr);
  EXPECT_EQ(idx->index_name, "idx_name");
  EXPECT_EQ(idx->table_name, "users");
}

} // namespace
} // namespace entropy
