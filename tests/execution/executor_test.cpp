/**
 * @file executor_test.cpp
 * @brief Tests for execution engine executors
 */

#include <gtest/gtest.h>

#include "catalog/catalog.hpp"
#include "execution/delete_executor.hpp"
#include "execution/filter.hpp"
#include "execution/insert_executor.hpp"
#include "execution/projection.hpp"
#include "execution/seq_scan_executor.hpp"
#include "parser/expression.hpp"
#include "storage/buffer_pool.hpp"
#include "test_utils.hpp"

namespace entropy {
namespace {

class ExecutorTest : public ::testing::Test {
protected:
  void SetUp() override {
    temp_file_ = std::make_unique<test::TempFile>("exec_test_");
    disk_manager_ = std::make_shared<DiskManager>(temp_file_->string());
    buffer_pool_ = std::make_shared<BufferPoolManager>(20, disk_manager_);
    catalog_ = std::make_unique<Catalog>(buffer_pool_);

    // Create test table
    schema_ = Schema({
        Column("id", TypeId::INTEGER),
        Column("name", TypeId::VARCHAR, 100),
        Column("age", TypeId::INTEGER),
    });
    ASSERT_TRUE(catalog_->create_table("users", schema_).ok());

    table_info_ = catalog_->get_table("users");
    ASSERT_NE(table_info_, nullptr);
  }

  void TearDown() override {
    table_info_ = nullptr;
    catalog_.reset();
    buffer_pool_.reset();
    disk_manager_.reset();
    temp_file_.reset();
  }

  // Helper to insert test data
  void insert_test_data() {
    std::vector<Tuple> tuples;
    tuples.emplace_back(
        std::vector<TupleValue>{TupleValue(int32_t(1)),
                                TupleValue(std::string("Alice")),
                                TupleValue(int32_t(25))},
        schema_);
    tuples.emplace_back(std::vector<TupleValue>{TupleValue(int32_t(2)),
                                                TupleValue(std::string("Bob")),
                                                TupleValue(int32_t(30))},
                        schema_);
    tuples.emplace_back(
        std::vector<TupleValue>{TupleValue(int32_t(3)),
                                TupleValue(std::string("Charlie")),
                                TupleValue(int32_t(35))},
        schema_);

    InsertExecutor insert(nullptr, table_info_->table_heap, &schema_,
                          std::move(tuples));
    insert.init();
    insert.next();
  }

  std::unique_ptr<test::TempFile> temp_file_;
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_;
  std::unique_ptr<Catalog> catalog_;
  TableInfo *table_info_ = nullptr;
  Schema schema_;
};

// ─────────────────────────────────────────────────────────────────────────────
// SeqScan Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(ExecutorTest, SeqScanEmpty) {
  SeqScanExecutor scan(nullptr, table_info_->table_heap, &schema_);
  scan.init();

  auto tuple = scan.next();
  EXPECT_FALSE(tuple.has_value());
}

TEST_F(ExecutorTest, SeqScanAll) {
  insert_test_data();

  SeqScanExecutor scan(nullptr, table_info_->table_heap, &schema_);
  scan.init();

  int count = 0;
  while (scan.next().has_value()) {
    count++;
  }
  EXPECT_EQ(count, 3);
}

TEST_F(ExecutorTest, SeqScanWithPredicate) {
  insert_test_data();

  // Predicate: age > 28
  auto age_col = std::make_unique<ColumnRefExpression>("age");
  age_col->set_column_index(2);
  age_col->set_type(TypeId::INTEGER);

  auto const_val =
      std::make_unique<ConstantExpression>(TupleValue(int32_t(28)));

  auto predicate = std::make_unique<ComparisonExpression>(
      ComparisonType::GREATER_THAN, std::move(age_col), std::move(const_val));

  SeqScanExecutor scan(nullptr, table_info_->table_heap, &schema_,
                       std::move(predicate));
  scan.init();

  int count = 0;
  while (auto tuple = scan.next()) {
    int32_t age = tuple->get_value(schema_, 2).as_integer();
    EXPECT_GT(age, 28);
    count++;
  }
  EXPECT_EQ(count, 2); // Bob (30) and Charlie (35)
}

// ─────────────────────────────────────────────────────────────────────────────
// Insert Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(ExecutorTest, InsertSingle) {
  std::vector<Tuple> tuples;
  tuples.emplace_back(std::vector<TupleValue>{TupleValue(int32_t(1)),
                                              TupleValue(std::string("Test")),
                                              TupleValue(int32_t(20))},
                      schema_);

  InsertExecutor insert(nullptr, table_info_->table_heap, &schema_,
                        std::move(tuples));
  insert.init();
  insert.next();

  EXPECT_EQ(insert.rows_inserted(), 1);

  // Verify insertion
  SeqScanExecutor scan(nullptr, table_info_->table_heap, &schema_);
  scan.init();
  auto tuple = scan.next();
  ASSERT_TRUE(tuple.has_value());
  EXPECT_EQ(tuple->get_value(schema_, 0).as_integer(), 1);
}

TEST_F(ExecutorTest, InsertMultiple) {
  std::vector<Tuple> tuples;
  for (int i = 0; i < 5; i++) {
    tuples.emplace_back(
        std::vector<TupleValue>{
            TupleValue(int32_t(i)),
            TupleValue(std::string("User" + std::to_string(i))),
            TupleValue(int32_t(20 + i))},
        schema_);
  }

  InsertExecutor insert(nullptr, table_info_->table_heap, &schema_,
                        std::move(tuples));
  insert.init();
  insert.next();

  EXPECT_EQ(insert.rows_inserted(), 5);

  // Count via scan
  SeqScanExecutor scan(nullptr, table_info_->table_heap, &schema_);
  scan.init();
  int count = 0;
  while (scan.next().has_value())
    count++;
  EXPECT_EQ(count, 5);
}

// ─────────────────────────────────────────────────────────────────────────────
// Delete Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(ExecutorTest, DeleteAll) {
  insert_test_data();

  // Verify data exists
  {
    SeqScanExecutor scan(nullptr, table_info_->table_heap, &schema_);
    scan.init();
    int count = 0;
    while (scan.next().has_value())
      count++;
    EXPECT_EQ(count, 3);
  }

  // Delete all (no predicate in child scan)
  auto child = std::make_unique<SeqScanExecutor>(
      nullptr, table_info_->table_heap, &schema_);
  DeleteExecutor del(nullptr, std::move(child), table_info_->table_heap);
  del.init();
  del.next();

  EXPECT_EQ(del.rows_deleted(), 3);
}

// ─────────────────────────────────────────────────────────────────────────────
// Filter Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(ExecutorTest, FilterTuples) {
  insert_test_data();

  // Create child scan
  auto child = std::make_unique<SeqScanExecutor>(
      nullptr, table_info_->table_heap, &schema_);

  // Predicate: id = 2
  auto id_col = std::make_unique<ColumnRefExpression>("id");
  id_col->set_column_index(0);
  id_col->set_type(TypeId::INTEGER);

  auto const_val = std::make_unique<ConstantExpression>(TupleValue(int32_t(2)));

  auto predicate = std::make_unique<ComparisonExpression>(
      ComparisonType::EQUAL, std::move(id_col), std::move(const_val));

  FilterExecutor filter(nullptr, std::move(child), std::move(predicate),
                        &schema_);
  filter.init();

  auto tuple = filter.next();
  ASSERT_TRUE(tuple.has_value());
  EXPECT_EQ(tuple->get_value(schema_, 0).as_integer(), 2);
  EXPECT_EQ(tuple->get_value(schema_, 1).as_string(), "Bob");

  // No more matching tuples
  EXPECT_FALSE(filter.next().has_value());
}

// ─────────────────────────────────────────────────────────────────────────────
// Projection Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(ExecutorTest, ProjectColumns) {
  insert_test_data();

  // Create child scan
  auto child = std::make_unique<SeqScanExecutor>(
      nullptr, table_info_->table_heap, &schema_);

  // Project only name and age (indices 1 and 2)
  std::vector<size_t> columns = {1, 2};

  ProjectionExecutor proj(nullptr, std::move(child), &schema_, columns);
  proj.init();

  EXPECT_EQ(proj.output_schema().column_count(), 2);
  EXPECT_EQ(proj.output_schema().column(0).name(), "name");
  EXPECT_EQ(proj.output_schema().column(1).name(), "age");

  auto tuple = proj.next();
  ASSERT_TRUE(tuple.has_value());

  // Verify projected tuple has only 2 columns
  EXPECT_EQ(tuple->get_value(proj.output_schema(), 0).as_string(), "Alice");
  EXPECT_EQ(tuple->get_value(proj.output_schema(), 1).as_integer(), 25);
}

} // namespace
} // namespace entropy
