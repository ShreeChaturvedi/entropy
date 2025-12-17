/**
 * @file executor_test.cpp
 * @brief Tests for execution engine executors
 */

#include <gtest/gtest.h>

#include "catalog/catalog.hpp"
#include "execution/aggregation.hpp"
#include "execution/delete_executor.hpp"
#include "execution/filter.hpp"
#include "execution/insert_executor.hpp"
#include "execution/nested_loop_join.hpp"
#include "execution/projection.hpp"
#include "execution/seq_scan_executor.hpp"
#include "execution/update_executor.hpp"
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

// ─────────────────────────────────────────────────────────────────────────────
// Update Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(ExecutorTest, UpdateTuples) {
  insert_test_data();

  // Create child scan with predicate: id = 2 (Bob)
  auto id_col = std::make_unique<ColumnRefExpression>("id");
  id_col->set_column_index(0);
  id_col->set_type(TypeId::INTEGER);
  auto const_val = std::make_unique<ConstantExpression>(TupleValue(int32_t(2)));
  auto predicate = std::make_unique<ComparisonExpression>(
      ComparisonType::EQUAL, std::move(id_col), std::move(const_val));

  auto child = std::make_unique<SeqScanExecutor>(
      nullptr, table_info_->table_heap, &schema_, std::move(predicate));

  // SET age = 99
  std::vector<size_t> col_indices = {2}; // age column
  std::vector<std::unique_ptr<Expression>> values;
  values.push_back(
      std::make_unique<ConstantExpression>(TupleValue(int32_t(99))));

  UpdateExecutor update(nullptr, std::move(child), table_info_->table_heap,
                        &schema_, std::move(col_indices), std::move(values));
  update.init();
  (void)update.next();

  EXPECT_EQ(update.rows_updated(), 1);

  // Verify update: find Bob and check age is 99
  SeqScanExecutor scan(nullptr, table_info_->table_heap, &schema_);
  scan.init();
  bool found_bob = false;
  while (auto tuple = scan.next()) {
    if (tuple->get_value(schema_, 0).as_integer() == 2) {
      EXPECT_EQ(tuple->get_value(schema_, 2).as_integer(), 99);
      found_bob = true;
    }
  }
  EXPECT_TRUE(found_bob);
}

// ─────────────────────────────────────────────────────────────────────────────
// Nested Loop Join Tests
// ─────────────────────────────────────────────────────────────────────────────

class JoinTest : public ::testing::Test {
protected:
  void SetUp() override {
    temp_file_ = std::make_unique<test::TempFile>("join_test_");
    disk_manager_ = std::make_shared<DiskManager>(temp_file_->string());
    buffer_pool_ = std::make_shared<BufferPoolManager>(20, disk_manager_);
    catalog_ = std::make_unique<Catalog>(buffer_pool_);

    // Create users table (id, name)
    users_schema_ = Schema({
        Column("user_id", TypeId::INTEGER),
        Column("name", TypeId::VARCHAR, 100),
    });
    ASSERT_TRUE(catalog_->create_table("users", users_schema_).ok());
    users_info_ = catalog_->get_table("users");

    // Create orders table (order_id, user_id, amount)
    orders_schema_ = Schema({
        Column("order_id", TypeId::INTEGER),
        Column("user_id", TypeId::INTEGER),
        Column("amount", TypeId::INTEGER),
    });
    ASSERT_TRUE(catalog_->create_table("orders", orders_schema_).ok());
    orders_info_ = catalog_->get_table("orders");

    // Create joined output schema
    output_schema_ = Schema({
        Column("user_id", TypeId::INTEGER),
        Column("name", TypeId::VARCHAR, 100),
        Column("order_id", TypeId::INTEGER),
        Column("order_user_id", TypeId::INTEGER),
        Column("amount", TypeId::INTEGER),
    });

    // Insert test data
    insert_users();
    insert_orders();
  }

  void insert_users() {
    std::vector<Tuple> tuples;
    tuples.emplace_back(
        std::vector<TupleValue>{TupleValue(int32_t(1)),
                                TupleValue(std::string("Alice"))},
        users_schema_);
    tuples.emplace_back(std::vector<TupleValue>{TupleValue(int32_t(2)),
                                                TupleValue(std::string("Bob"))},
                        users_schema_);
    tuples.emplace_back(
        std::vector<TupleValue>{TupleValue(int32_t(3)),
                                TupleValue(std::string("Charlie"))},
        users_schema_);

    InsertExecutor insert(nullptr, users_info_->table_heap, &users_schema_,
                          std::move(tuples));
    insert.init();
    (void)insert.next();
  }

  void insert_orders() {
    std::vector<Tuple> tuples;
    // Alice has 2 orders, Bob has 1, Charlie has none
    tuples.emplace_back(std::vector<TupleValue>{TupleValue(int32_t(101)),
                                                TupleValue(int32_t(1)),
                                                TupleValue(int32_t(100))},
                        orders_schema_);
    tuples.emplace_back(std::vector<TupleValue>{TupleValue(int32_t(102)),
                                                TupleValue(int32_t(1)),
                                                TupleValue(int32_t(200))},
                        orders_schema_);
    tuples.emplace_back(std::vector<TupleValue>{TupleValue(int32_t(103)),
                                                TupleValue(int32_t(2)),
                                                TupleValue(int32_t(150))},
                        orders_schema_);
    // Order 104 for user_id 99 (no matching user)
    tuples.emplace_back(std::vector<TupleValue>{TupleValue(int32_t(104)),
                                                TupleValue(int32_t(99)),
                                                TupleValue(int32_t(50))},
                        orders_schema_);

    InsertExecutor insert(nullptr, orders_info_->table_heap, &orders_schema_,
                          std::move(tuples));
    insert.init();
    (void)insert.next();
  }

  std::unique_ptr<test::TempFile> temp_file_;
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_;
  std::unique_ptr<Catalog> catalog_;
  TableInfo *users_info_ = nullptr;
  TableInfo *orders_info_ = nullptr;
  Schema users_schema_;
  Schema orders_schema_;
  Schema output_schema_;
};

TEST_F(JoinTest, InnerJoin) {
  auto left = std::make_unique<SeqScanExecutor>(
      nullptr, users_info_->table_heap, &users_schema_);
  auto right = std::make_unique<SeqScanExecutor>(
      nullptr, orders_info_->table_heap, &orders_schema_);

  // Join condition: users.user_id = orders.user_id
  auto left_col = std::make_unique<ColumnRefExpression>("user_id");
  left_col->set_column_index(0);
  left_col->set_type(TypeId::INTEGER);

  auto right_col = std::make_unique<ColumnRefExpression>("order_user_id");
  right_col->set_column_index(3); // Index in combined schema
  right_col->set_type(TypeId::INTEGER);

  auto condition = std::make_unique<ComparisonExpression>(
      ComparisonType::EQUAL, std::move(left_col), std::move(right_col));

  NestedLoopJoinExecutor join(nullptr, std::move(left), std::move(right),
                              &users_schema_, &orders_schema_, &output_schema_,
                              JoinType::INNER, std::move(condition));
  join.init();

  int count = 0;
  while (auto tuple = join.next()) {
    // Verify join condition: left user_id == right user_id
    int32_t left_id = tuple->get_value(output_schema_, 0).as_integer();
    int32_t right_id = tuple->get_value(output_schema_, 3).as_integer();
    EXPECT_EQ(left_id, right_id);
    count++;
  }

  // Alice has 2 orders, Bob has 1 = 3 matching rows
  EXPECT_EQ(count, 3);
}

TEST_F(JoinTest, LeftJoin) {
  auto left = std::make_unique<SeqScanExecutor>(
      nullptr, users_info_->table_heap, &users_schema_);
  auto right = std::make_unique<SeqScanExecutor>(
      nullptr, orders_info_->table_heap, &orders_schema_);

  // Join condition: users.user_id = orders.user_id
  auto left_col = std::make_unique<ColumnRefExpression>("user_id");
  left_col->set_column_index(0);
  left_col->set_type(TypeId::INTEGER);

  auto right_col = std::make_unique<ColumnRefExpression>("order_user_id");
  right_col->set_column_index(3);
  right_col->set_type(TypeId::INTEGER);

  auto condition = std::make_unique<ComparisonExpression>(
      ComparisonType::EQUAL, std::move(left_col), std::move(right_col));

  NestedLoopJoinExecutor join(nullptr, std::move(left), std::move(right),
                              &users_schema_, &orders_schema_, &output_schema_,
                              JoinType::LEFT, std::move(condition));
  join.init();

  int count = 0;
  bool found_charlie = false;
  while (auto tuple = join.next()) {
    std::string name = tuple->get_value(output_schema_, 1).as_string();
    if (name == "Charlie") {
      // Charlie has no orders, right side should be NULL
      TupleValue order_id = tuple->get_value(output_schema_, 2);
      EXPECT_TRUE(order_id.is_null());
      found_charlie = true;
    }
    count++;
  }

  // Alice:2 + Bob:1 + Charlie:1(null) = 4 rows
  EXPECT_EQ(count, 4);
  EXPECT_TRUE(found_charlie);
}

TEST_F(JoinTest, CrossJoin) {
  auto left = std::make_unique<SeqScanExecutor>(
      nullptr, users_info_->table_heap, &users_schema_);
  auto right = std::make_unique<SeqScanExecutor>(
      nullptr, orders_info_->table_heap, &orders_schema_);

  // No condition for CROSS JOIN
  NestedLoopJoinExecutor join(nullptr, std::move(left), std::move(right),
                              &users_schema_, &orders_schema_, &output_schema_,
                              JoinType::CROSS, nullptr);
  join.init();

  int count = 0;
  while (join.next().has_value()) {
    count++;
  }

  // 3 users × 4 orders = 12 rows
  EXPECT_EQ(count, 12);
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation Tests
// ─────────────────────────────────────────────────────────────────────────────

class AggregationTest : public ::testing::Test {
protected:
  void SetUp() override {
    temp_file_ = std::make_unique<test::TempFile>("agg_test_");
    disk_manager_ = std::make_shared<DiskManager>(temp_file_->string());
    buffer_pool_ = std::make_shared<BufferPoolManager>(20, disk_manager_);
    catalog_ = std::make_unique<Catalog>(buffer_pool_);

    // Create sales table: product, category, price, quantity
    schema_ = Schema({
        Column("product", TypeId::VARCHAR, 50),
        Column("category", TypeId::VARCHAR, 20),
        Column("price", TypeId::INTEGER),
        Column("quantity", TypeId::INTEGER),
    });
    ASSERT_TRUE(catalog_->create_table("sales", schema_).ok());
    table_info_ = catalog_->get_table("sales");

    insert_test_data();
  }

  void insert_test_data() {
    std::vector<Tuple> tuples;
    // Electronics: Laptop $1000 x 2, Phone $500 x 5
    tuples.emplace_back(std::vector<TupleValue>{
        TupleValue(std::string("Laptop")), TupleValue(std::string("Electronics")),
        TupleValue(int32_t(1000)), TupleValue(int32_t(2))}, schema_);
    tuples.emplace_back(std::vector<TupleValue>{
        TupleValue(std::string("Phone")), TupleValue(std::string("Electronics")),
        TupleValue(int32_t(500)), TupleValue(int32_t(5))}, schema_);
    // Books: Novel $20 x 10, Textbook $80 x 3
    tuples.emplace_back(std::vector<TupleValue>{
        TupleValue(std::string("Novel")), TupleValue(std::string("Books")),
        TupleValue(int32_t(20)), TupleValue(int32_t(10))}, schema_);
    tuples.emplace_back(std::vector<TupleValue>{
        TupleValue(std::string("Textbook")), TupleValue(std::string("Books")),
        TupleValue(int32_t(80)), TupleValue(int32_t(3))}, schema_);
    // Clothing: Shirt $30 x 7
    tuples.emplace_back(std::vector<TupleValue>{
        TupleValue(std::string("Shirt")), TupleValue(std::string("Clothing")),
        TupleValue(int32_t(30)), TupleValue(int32_t(7))}, schema_);

    InsertExecutor insert(nullptr, table_info_->table_heap, &schema_,
                          std::move(tuples));
    insert.init();
    (void)insert.next();
  }

  std::unique_ptr<test::TempFile> temp_file_;
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_;
  std::unique_ptr<Catalog> catalog_;
  TableInfo* table_info_ = nullptr;
  Schema schema_;
};

TEST_F(AggregationTest, CountStar) {
  auto child = std::make_unique<SeqScanExecutor>(
      nullptr, table_info_->table_heap, &schema_);

  std::vector<AggregateExpression> aggs = {
      AggregateExpression::count_star("total")
  };

  AggregationExecutor agg(nullptr, std::move(child), &schema_, {}, std::move(aggs));
  agg.init();

  auto result = agg.next();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->get_value(agg.output_schema(), 0).as_bigint(), 5);

  // Only one row for scalar aggregation
  EXPECT_FALSE(agg.next().has_value());
}

TEST_F(AggregationTest, SumAvgMinMax) {
  auto child = std::make_unique<SeqScanExecutor>(
      nullptr, table_info_->table_heap, &schema_);

  std::vector<AggregateExpression> aggs = {
      AggregateExpression::sum(2, "sum_price"),   // price column
      AggregateExpression::avg(2, "avg_price"),
      AggregateExpression::min(2, "min_price"),
      AggregateExpression::max(2, "max_price"),
  };

  AggregationExecutor agg(nullptr, std::move(child), &schema_, {}, std::move(aggs));
  agg.init();

  auto result = agg.next();
  ASSERT_TRUE(result.has_value());

  // Sum: 1000 + 500 + 20 + 80 + 30 = 1630
  EXPECT_EQ(result->get_value(agg.output_schema(), 0).as_bigint(), 1630);
  // Avg: 1630 / 5 = 326
  EXPECT_DOUBLE_EQ(result->get_value(agg.output_schema(), 1).as_double(), 326.0);
  // Min: 20
  EXPECT_EQ(result->get_value(agg.output_schema(), 2).as_integer(), 20);
  // Max: 1000
  EXPECT_EQ(result->get_value(agg.output_schema(), 3).as_integer(), 1000);
}

TEST_F(AggregationTest, GroupByCategory) {
  auto child = std::make_unique<SeqScanExecutor>(
      nullptr, table_info_->table_heap, &schema_);

  // GROUP BY category, COUNT(*), SUM(price)
  std::vector<size_t> group_by = {1};  // category column
  std::vector<AggregateExpression> aggs = {
      AggregateExpression::count_star("cnt"),
      AggregateExpression::sum(2, "sum_price"),
  };

  AggregationExecutor agg(nullptr, std::move(child), &schema_,
                           std::move(group_by), std::move(aggs));
  agg.init();

  std::map<std::string, std::pair<int64_t, int64_t>> results;
  while (auto result = agg.next()) {
    std::string cat = result->get_value(agg.output_schema(), 0).as_string();
    int64_t cnt = result->get_value(agg.output_schema(), 1).as_bigint();
    int64_t sum = result->get_value(agg.output_schema(), 2).as_bigint();
    results[cat] = {cnt, sum};
  }

  EXPECT_EQ(results.size(), 3);  // 3 categories

  // Electronics: 2 items, 1000 + 500 = 1500
  EXPECT_EQ(results["Electronics"].first, 2);
  EXPECT_EQ(results["Electronics"].second, 1500);

  // Books: 2 items, 20 + 80 = 100
  EXPECT_EQ(results["Books"].first, 2);
  EXPECT_EQ(results["Books"].second, 100);

  // Clothing: 1 item, 30
  EXPECT_EQ(results["Clothing"].first, 1);
  EXPECT_EQ(results["Clothing"].second, 30);
}

TEST_F(AggregationTest, EmptyTable) {
  // Create empty table
  Schema empty_schema({Column("x", TypeId::INTEGER)});
  ASSERT_TRUE(catalog_->create_table("empty", empty_schema).ok());
  auto empty_info = catalog_->get_table("empty");

  auto child = std::make_unique<SeqScanExecutor>(
      nullptr, empty_info->table_heap, &empty_schema);

  std::vector<AggregateExpression> aggs = {
      AggregateExpression::count_star("cnt"),
      AggregateExpression::sum(0, "sum"),
  };

  AggregationExecutor agg(nullptr, std::move(child), &empty_schema, {}, std::move(aggs));
  agg.init();

  auto result = agg.next();
  ASSERT_TRUE(result.has_value());

  // COUNT(*) on empty table = 0
  EXPECT_EQ(result->get_value(agg.output_schema(), 0).as_bigint(), 0);
  // SUM on empty table = NULL
  EXPECT_TRUE(result->get_value(agg.output_schema(), 1).is_null());
}

TEST_F(AggregationTest, MultipleGroupByColumns) {
  auto child = std::make_unique<SeqScanExecutor>(
      nullptr, table_info_->table_heap, &schema_);

  // GROUP BY category, price (each product is unique by this combo)
  std::vector<size_t> group_by = {1, 2};  // category, price
  std::vector<AggregateExpression> aggs = {
      AggregateExpression::count_star("cnt"),
  };

  AggregationExecutor agg(nullptr, std::move(child), &schema_,
                           std::move(group_by), std::move(aggs));
  agg.init();

  int count = 0;
  while (agg.next().has_value()) {
    count++;
  }

  // Each row has unique (category, price), so 5 groups
  EXPECT_EQ(count, 5);
}

}  // namespace
}  // namespace entropy
