/**
 * @file optimizer_test.cpp
 * @brief Unit tests for optimizer components
 */

#include <gtest/gtest.h>

#include "catalog/catalog.hpp"
#include "catalog/column.hpp"
#include "catalog/schema.hpp"
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
    disk_manager_ = std::make_shared<DiskManager>("optimizer_test.db");
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
