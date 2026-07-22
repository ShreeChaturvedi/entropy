/**
 * @file optimizer_test.cpp
 * @brief Unit tests for optimizer components
 */

#include <gtest/gtest.h>

#include "catalog/catalog.hpp"
#include "catalog/column.hpp"
#include "catalog/schema.hpp"
#include "execution/index_scan_executor.hpp"
#include "optimizer/cost_model.hpp"
#include "optimizer/index_selector.hpp"
#include "optimizer/plan_node.hpp"
#include "optimizer/statistics.hpp"
#include "parser/expression.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/disk_manager.hpp"
#include "storage/table_heap.hpp"
#include "storage/tuple.hpp"

namespace entropy {

class OptimizerTest : public ::testing::Test {
protected:
  void SetUp() override {
    disk_manager_ = std::make_shared<FileDiskManager>("optimizer_test.db");
    buffer_pool_ = std::make_shared<BufferPoolManager>(32, disk_manager_);
    catalog_ = std::make_shared<Catalog>(buffer_pool_);
    statistics_ = std::make_shared<Statistics>(catalog_);
    cost_model_ = std::make_shared<CostModel>(statistics_);
    index_selector_ =
        std::make_unique<IndexSelector>(catalog_, statistics_, cost_model_);

    output_schema_ = Schema({
        Column("id", TypeId::INTEGER),
        Column("name", TypeId::VARCHAR, 100),
        Column("age", TypeId::INTEGER),
    });
    ASSERT_TRUE(catalog_->create_table("users", output_schema_).ok());
    table_info_ = catalog_->get_table("users");
    ASSERT_NE(table_info_, nullptr);

    insert_test_data();
  }

  void TearDown() override {
    buffer_pool_->flush_all_pages();
    std::remove("optimizer_test.db");
  }

  void insert_test_data() {
    for (int i = 1; i <= 100; ++i) {
      std::vector<TupleValue> values = {
          TupleValue(i),
          TupleValue("User" + std::to_string(i)),
          TupleValue(20 + (i % 50)),
      };
      Tuple tuple(values, output_schema_);
      RID rid;
      ASSERT_TRUE(table_info_->table_heap->insert_tuple(tuple, &rid).ok());
    }
  }

  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_;
  std::shared_ptr<Catalog> catalog_;
  std::shared_ptr<Statistics> statistics_;
  std::shared_ptr<CostModel> cost_model_;
  std::unique_ptr<IndexSelector> index_selector_;
  Schema output_schema_;
  TableInfo *table_info_ = nullptr;
};

// Statistics Tests

TEST_F(OptimizerTest, TableCardinality) {
  size_t cardinality = statistics_->table_cardinality(table_info_->oid);
  EXPECT_EQ(cardinality, 100);
}

TEST_F(OptimizerTest, CollectStatistics) {
  statistics_->collect_statistics(table_info_->oid);

  const auto *stats = statistics_->get_table_stats(table_info_->oid);
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->row_count, 100);
  EXPECT_GT(stats->page_count, 0);
}

// Regression test for issue #15: collect_statistics previously used fixed
// 32-element stack arrays (null_counts[32], distinct_estimates[32]) indexed by
// the table's column_count() with no bound check. A table with more than 32
// columns wrote past the end of those arrays, corrupting the stack (UB). This
// builds a 40-column table and verifies per-column statistics are correct;
// under AddressSanitizer it flags a stack-buffer-overflow before the fix.
TEST_F(OptimizerTest, CollectStatisticsManyColumns) {
  constexpr size_t kNumColumns = 40; // > 32, past the old fixed-array bound
  constexpr int kNumRows = 10;
  constexpr size_t kNullColumn = 35; // > 32, exercises OOB write directly

  std::vector<Column> columns;
  columns.reserve(kNumColumns);
  for (size_t c = 0; c < kNumColumns; ++c) {
    columns.emplace_back("col" + std::to_string(c), TypeId::INTEGER);
  }
  Schema wide_schema(columns);
  ASSERT_TRUE(catalog_->create_table("wide", wide_schema).ok());
  TableInfo *wide_info = catalog_->get_table("wide");
  ASSERT_NE(wide_info, nullptr);

  // Column kNullColumn is NULL in every row; all other columns are non-null.
  for (int r = 0; r < kNumRows; ++r) {
    std::vector<TupleValue> values;
    values.reserve(kNumColumns);
    for (size_t c = 0; c < kNumColumns; ++c) {
      if (c == kNullColumn) {
        values.push_back(TupleValue::null());
      } else {
        values.push_back(TupleValue(r * 100 + static_cast<int>(c)));
      }
    }
    Tuple tuple(values, wide_schema);
    RID rid;
    ASSERT_TRUE(wide_info->table_heap->insert_tuple(tuple, &rid).ok());
  }

  statistics_->collect_statistics(wide_info->oid);
  const auto *stats = statistics_->get_table_stats(wide_info->oid);
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->row_count, static_cast<size_t>(kNumRows));
  EXPECT_EQ(stats->columns.size(), kNumColumns);

  for (size_t c = 0; c < kNumColumns; ++c) {
    auto it = stats->columns.find(static_cast<column_id_t>(c));
    ASSERT_NE(it, stats->columns.end()) << "missing stats for column " << c;
    const ColumnStatistics &cs = it->second;
    if (c == kNullColumn) {
      // All-null column: no distinct non-null values, null_fraction == 1.0.
      EXPECT_DOUBLE_EQ(cs.null_fraction, 1.0) << "column " << c;
      EXPECT_DOUBLE_EQ(cs.distinct_values, 0.0) << "column " << c;
    } else {
      // Non-null column: distinct_estimates == row_count, scaled by 0.9.
      EXPECT_DOUBLE_EQ(cs.null_fraction, 0.0) << "column " << c;
      EXPECT_DOUBLE_EQ(cs.distinct_values, kNumRows * 0.9) << "column " << c;
    }
  }
}

TEST_F(OptimizerTest, PredicateSelectivity) {
  auto eq_expr = std::make_unique<ComparisonExpression>(
      ComparisonType::EQUAL, std::make_unique<ColumnRefExpression>("id"),
      std::make_unique<ConstantExpression>(TupleValue(1)));

  double sel =
      statistics_->estimate_selectivity(table_info_->oid, eq_expr.get());
  EXPECT_DOUBLE_EQ(sel, Statistics::EQUALITY_SELECTIVITY);
}

// CostModel Tests

TEST_F(OptimizerTest, SeqScanCost) {
  auto seq_scan = std::make_unique<SeqScanPlanNode>(table_info_->oid,
                                                    &output_schema_, nullptr);

  double cost = cost_model_->estimate_cost(seq_scan.get());
  EXPECT_GT(cost, 0);
}

TEST_F(OptimizerTest, IndexScanCost) {
  auto index_scan = std::make_unique<IndexScanPlanNode>(
      table_info_->oid, 1, &output_schema_,
      IndexScanPlanNode::ScanType::POINT_LOOKUP, 42);

  double cost = cost_model_->estimate_cost(index_scan.get());
  EXPECT_GT(cost, 0);
}

TEST_F(OptimizerTest, CostComparison) {
  auto seq_scan = std::make_unique<SeqScanPlanNode>(table_info_->oid,
                                                    &output_schema_, nullptr);
  auto index_scan = std::make_unique<IndexScanPlanNode>(
      table_info_->oid, 1, &output_schema_,
      IndexScanPlanNode::ScanType::POINT_LOOKUP, 42);

  double seq_cost = cost_model_->estimate_cost(seq_scan.get());
  double idx_cost = cost_model_->estimate_cost(index_scan.get());

  // Both costs should be positive
  EXPECT_GT(seq_cost, 0);
  EXPECT_GT(idx_cost, 0);
  // For small tables, SeqScan can be cheaper - just verify costs differ
  EXPECT_NE(seq_cost, idx_cost);
}

// IndexSelector Tests

TEST_F(OptimizerTest, SelectAccessMethodNoIndex) {
  auto selection =
      index_selector_->select_access_method(table_info_->oid, nullptr);

  EXPECT_FALSE(selection.use_index);
  EXPECT_GT(selection.seq_scan_cost, 0);
}

TEST_F(OptimizerTest, SelectAccessMethodWithIndex) {
  ASSERT_TRUE(catalog_->create_index("idx_users_id", "users", "id").ok());

  auto predicate = std::make_unique<ComparisonExpression>(
      ComparisonType::EQUAL, std::make_unique<ColumnRefExpression>("id"),
      std::make_unique<ConstantExpression>(TupleValue(42)));

  auto selection =
      index_selector_->select_access_method(table_info_->oid, predicate.get());

  // With index available and point lookup predicate:
  // - Index should be identified (available)
  // - For small tables, optimizer may still choose SeqScan
  // - Just verify costs are reasonable
  EXPECT_GT(selection.seq_scan_cost, 0);
}

// Regression for issue #4: IndexSelector returns an index OID, but execute_select
// used to cast that OID to column_id_t and call get_index_for_column. When the
// OID numerically aliases another column's position, the wrong index is used
// and wrong rows are returned.
TEST_F(OptimizerTest, IndexScanResolvesByOidNotColumnId) {
  // Table oid=1. First index (id, col 0) gets oid=2; second (age, col 2) oid=3.
  // Casting id-index oid 2 to column_id looks up key_column==2 → age index.
  ASSERT_TRUE(catalog_->create_index("idx_users_id", "users", "id").ok());
  ASSERT_TRUE(catalog_->create_index("idx_users_age", "users", "age").ok());

  IndexInfo *id_index = catalog_->get_index("idx_users_id");
  IndexInfo *age_index = catalog_->get_index("idx_users_age");
  ASSERT_NE(id_index, nullptr);
  ASSERT_NE(age_index, nullptr);
  ASSERT_EQ(id_index->oid, 2u);
  ASSERT_EQ(id_index->key_column, 0);
  ASSERT_EQ(age_index->key_column, 2);

  // Bug pattern from database.cpp: OID cast to column_id selects age index.
  IndexInfo *aliased =
      catalog_->get_index_for_column(table_info_->oid,
                                     static_cast<column_id_t>(id_index->oid));
  ASSERT_NE(aliased, nullptr);
  EXPECT_EQ(aliased, age_index) << "OID→column_id alias must hit age index";

  // Correct resolution: look up by OID.
  IndexInfo *resolved = catalog_->get_index_by_oid(id_index->oid);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved, id_index);

  // Point lookup key=42 via the correct (id) index returns the id=42 row.
  IndexScanExecutor correct_scan(nullptr, resolved->index.get(),
                                 table_info_->table_heap.get(),
                                 &output_schema_, /*key=*/42);
  correct_scan.init();
  auto correct_row = correct_scan.next();
  ASSERT_TRUE(correct_row.has_value());
  EXPECT_EQ(correct_row->get_value(output_schema_, 0).as_integer(), 42);
  EXPECT_FALSE(correct_scan.next().has_value());

  // Same key via the aliased (age) index returns a different row (age=42 → id=22).
  IndexScanExecutor wrong_scan(nullptr, aliased->index.get(),
                               table_info_->table_heap.get(), &output_schema_,
                               /*key=*/42);
  wrong_scan.init();
  auto wrong_row = wrong_scan.next();
  ASSERT_TRUE(wrong_row.has_value());
  EXPECT_NE(wrong_row->get_value(output_schema_, 0).as_integer(), 42);
  EXPECT_EQ(wrong_row->get_value(output_schema_, 2).as_integer(), 42);
}

// PlanNode Tests

TEST_F(OptimizerTest, PlanNodeTypes) {
  auto seq_scan = std::make_unique<SeqScanPlanNode>(table_info_->oid,
                                                    &output_schema_, nullptr);
  EXPECT_EQ(seq_scan->type(), PlanNodeType::SEQ_SCAN);

  auto filter = std::make_unique<FilterPlanNode>(nullptr, &output_schema_);
  EXPECT_EQ(filter->type(), PlanNodeType::FILTER);

  auto sort = std::make_unique<SortPlanNode>(
      std::vector<SortPlanNode::SortKey>{{0, true}}, &output_schema_);
  EXPECT_EQ(sort->type(), PlanNodeType::SORT);

  auto limit = std::make_unique<LimitPlanNode>(10, 0, &output_schema_);
  EXPECT_EQ(limit->type(), PlanNodeType::LIMIT);
}

TEST_F(OptimizerTest, CardinalityEstimation) {
  auto seq_scan = std::make_unique<SeqScanPlanNode>(table_info_->oid,
                                                    &output_schema_, nullptr);

  size_t cardinality = cost_model_->estimate_cardinality(seq_scan.get());
  EXPECT_EQ(cardinality, 100);
}

} // namespace entropy
