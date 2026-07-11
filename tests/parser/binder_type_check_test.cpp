/**
 * @file binder_type_check_test.cpp
 * @brief Type-checking tests for the binder and expression evaluation (issue
 *        #34): bind-time type checks reject silent coercions, and evaluation
 *        never invokes undefined behaviour on integer overflow.
 */

#include <gtest/gtest.h>

#include <limits>
#include <memory>

#include "catalog/catalog.hpp"
#include "catalog/schema.hpp"
#include "parser/binder.hpp"
#include "parser/expression.hpp"
#include "parser/parser.hpp"
#include "parser/statement.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/disk_manager.hpp"
#include "storage/tuple.hpp"
#include "test_utils.hpp"

namespace entropy {
namespace {

// ─────────────────────────────────────────────────────────────────────────────
// Bind-time type checking
// ─────────────────────────────────────────────────────────────────────────────

class BinderTypeCheckTest : public ::testing::Test {
protected:
  void SetUp() override {
    temp_file_ = std::make_unique<test::TempFile>("binder_type_");
    disk_manager_ = std::make_shared<FileDiskManager>(temp_file_->string());
    buffer_pool_ = std::make_shared<BufferPoolManager>(16, disk_manager_);
    catalog_ = std::make_unique<Catalog>(buffer_pool_);

    Schema schema({
        Column("id", TypeId::INTEGER),
        Column("n", TypeId::INTEGER),
        Column("name", TypeId::VARCHAR, 32),
    });
    ASSERT_TRUE(catalog_->create_table("t", schema).ok());
  }

  void TearDown() override {
    catalog_.reset();
    buffer_pool_.reset();
    disk_manager_.reset();
    temp_file_.reset();
  }

  // Parse a statement and return it, or nullptr on parse failure.
  std::unique_ptr<Statement> parse(const std::string &sql) {
    Parser parser(sql);
    std::unique_ptr<Statement> stmt;
    if (!parser.parse(&stmt).ok()) {
      return nullptr;
    }
    return stmt;
  }

  Status bind_insert(const std::string &sql, bool strict) {
    auto stmt = parse(sql);
    EXPECT_NE(stmt, nullptr) << "parse failed: " << sql;
    auto *insert = dynamic_cast<InsertStatement *>(stmt.get());
    EXPECT_NE(insert, nullptr);
    Binder binder(catalog_.get(), strict);
    BoundInsertContext ctx;
    return binder.bind_insert(insert, &ctx);
  }

  Status bind_update(const std::string &sql, bool strict) {
    auto stmt = parse(sql);
    EXPECT_NE(stmt, nullptr) << "parse failed: " << sql;
    auto *update = dynamic_cast<UpdateStatement *>(stmt.get());
    EXPECT_NE(update, nullptr);
    Binder binder(catalog_.get(), strict);
    BoundUpdateContext ctx;
    return binder.bind_update(update, &ctx);
  }

  Status bind_select(const std::string &sql) {
    auto stmt = parse(sql);
    EXPECT_NE(stmt, nullptr) << "parse failed: " << sql;
    auto *select = dynamic_cast<SelectStatement *>(stmt.get());
    EXPECT_NE(select, nullptr);
    Binder binder(catalog_.get(), /*strict_mode=*/false);
    BoundSelectContext ctx;
    return binder.bind_select(select, &ctx);
  }

  std::unique_ptr<test::TempFile> temp_file_;
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_;
  std::unique_ptr<Catalog> catalog_;
};

TEST_F(BinderTypeCheckTest, StrictInsertRejectsStringIntoIntegerColumn) {
  EXPECT_FALSE(bind_insert("INSERT INTO t VALUES (1, 'oops', 'x')", true).ok())
      << "string accepted into INTEGER column in strict mode";
}

TEST_F(BinderTypeCheckTest, StrictInsertRejectsIntegerIntoVarcharColumn) {
  EXPECT_FALSE(bind_insert("INSERT INTO t VALUES (1, 2, 3)", true).ok())
      << "integer accepted into VARCHAR column in strict mode";
}

TEST_F(BinderTypeCheckTest, StrictInsertAcceptsWellTypedAndNull) {
  EXPECT_TRUE(bind_insert("INSERT INTO t VALUES (1, 2, 'ok')", true).ok());
  EXPECT_TRUE(bind_insert("INSERT INTO t VALUES (1, NULL, 'ok')", true).ok());
}

TEST_F(BinderTypeCheckTest, StrictUpdateRejectsStringIntoIntegerColumn) {
  EXPECT_FALSE(bind_update("UPDATE t SET n = 'text'", true).ok())
      << "string assigned to INTEGER column in strict mode";
}

TEST_F(BinderTypeCheckTest, StrictUpdateAcceptsWellTyped) {
  EXPECT_TRUE(bind_update("UPDATE t SET n = 5", true).ok());
  EXPECT_TRUE(bind_update("UPDATE t SET name = 'hi'", true).ok());
}

TEST_F(BinderTypeCheckTest, ArithmeticWithStringOperandIsRejected) {
  // The string literal is not coerced to 0; the whole predicate fails to bind.
  EXPECT_FALSE(bind_select("SELECT * FROM t WHERE n + 'x' = 1").ok())
      << "string operand accepted in arithmetic";
}

TEST_F(BinderTypeCheckTest, ArithmeticWithNumericOperandsBinds) {
  EXPECT_TRUE(bind_select("SELECT * FROM t WHERE n + 1 = id").ok());
}

TEST_F(BinderTypeCheckTest, CrossFamilyComparisonIsRejected) {
  EXPECT_FALSE(bind_select("SELECT * FROM t WHERE name = 1").ok())
      << "string column compared to integer literal";
  EXPECT_FALSE(bind_select("SELECT * FROM t WHERE id = 'x'").ok())
      << "integer column compared to string literal";
}

TEST_F(BinderTypeCheckTest, SameFamilyComparisonBinds) {
  EXPECT_TRUE(bind_select("SELECT * FROM t WHERE name = 'a'").ok());
  EXPECT_TRUE(bind_select("SELECT * FROM t WHERE id = 3").ok());
}

// ─────────────────────────────────────────────────────────────────────────────
// Safe evaluation: no undefined behaviour, no silent string->0 coercion
// ─────────────────────────────────────────────────────────────────────────────

// Constants ignore the tuple/schema, so a minimal schema and empty tuple are
// sufficient to drive BinaryOpExpression::evaluate directly.
Schema dummy_schema() { return Schema({Column("c", TypeId::BIGINT)}); }

std::unique_ptr<Expression> constant_bigint(int64_t v) {
  return std::make_unique<ConstantExpression>(TupleValue(v));
}

TupleValue eval_binary(BinaryOpType op, std::unique_ptr<Expression> l,
                       std::unique_ptr<Expression> r) {
  BinaryOpExpression expr(op, std::move(l), std::move(r));
  Schema schema = dummy_schema();
  Tuple tuple;
  return expr.evaluate(tuple, schema);
}

TEST(ExpressionOverflowTest, IntegerMultiplyOverflowYieldsNullNotUB) {
  const int64_t max = std::numeric_limits<int64_t>::max();
  TupleValue result = eval_binary(BinaryOpType::MULTIPLY, constant_bigint(max),
                                  constant_bigint(max));
  EXPECT_TRUE(result.is_null());
}

TEST(ExpressionOverflowTest, IntegerAddOverflowYieldsNullNotUB) {
  const int64_t max = std::numeric_limits<int64_t>::max();
  TupleValue result =
      eval_binary(BinaryOpType::ADD, constant_bigint(max), constant_bigint(1));
  EXPECT_TRUE(result.is_null());
}

TEST(ExpressionOverflowTest, IntegerSubtractOverflowYieldsNullNotUB) {
  const int64_t min = std::numeric_limits<int64_t>::min();
  TupleValue result =
      eval_binary(BinaryOpType::SUBTRACT, constant_bigint(min),
                  constant_bigint(1));
  EXPECT_TRUE(result.is_null());
}

TEST(ExpressionOverflowTest, IntMinDividedByMinusOneYieldsNullNotUB) {
  const int64_t min = std::numeric_limits<int64_t>::min();
  TupleValue result =
      eval_binary(BinaryOpType::DIVIDE, constant_bigint(min),
                  constant_bigint(-1));
  EXPECT_TRUE(result.is_null());
}

TEST(ExpressionOverflowTest, InRangeArithmeticStillComputes) {
  TupleValue result =
      eval_binary(BinaryOpType::MULTIPLY, constant_bigint(1000),
                  constant_bigint(1000));
  ASSERT_TRUE(result.is_bigint());
  EXPECT_EQ(result.as_bigint(), 1000000);
}

TEST(ExpressionCoercionTest, StringOperandInArithmeticIsNotCoercedToZero) {
  // 5 + 'abc' must not evaluate to 5 (string coerced to 0). It yields NULL.
  auto str = std::make_unique<ConstantExpression>(TupleValue(std::string("abc")));
  TupleValue result =
      eval_binary(BinaryOpType::ADD, constant_bigint(5), std::move(str));
  EXPECT_TRUE(result.is_null());
}

} // namespace
} // namespace entropy
