/**
 * @file optimizer_test.cpp
 * @brief Unit tests for optimizer components
 */

#include <gtest/gtest.h>

#include <cmath>

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
      // All-null column: no distinct non-null values, null_fraction == 1.0,
      // and no min/max were observed.
      EXPECT_DOUBLE_EQ(cs.null_fraction, 1.0) << "column " << c;
      EXPECT_DOUBLE_EQ(cs.distinct_values, 0.0) << "column " << c;
      EXPECT_FALSE(cs.min_value.has_value()) << "column " << c;
      EXPECT_FALSE(cs.max_value.has_value()) << "column " << c;
    } else {
      // Non-null column: every row holds a distinct value (r*100 + c), so the
      // exact distinct-value count is kNumRows (not the old ~0.9 * rows fudge),
      // with min at r=0 and max at r=kNumRows-1.
      EXPECT_DOUBLE_EQ(cs.null_fraction, 0.0) << "column " << c;
      EXPECT_DOUBLE_EQ(cs.distinct_values, static_cast<double>(kNumRows))
          << "column " << c;
      ASSERT_TRUE(cs.min_value.has_value()) << "column " << c;
      ASSERT_TRUE(cs.max_value.has_value()) << "column " << c;
      EXPECT_EQ(cs.min_value->as_integer(), static_cast<int>(c)) << "column "
                                                                 << c;
      EXPECT_EQ(cs.max_value->as_integer(),
                (kNumRows - 1) * 100 + static_cast<int>(c))
          << "column " << c;
    }
  }
}

// Issue #12: NDV was computed as 0.9 * non_null_rows, so a low-cardinality
// column over many rows reported a nonsensically high distinct count (e.g. a
// boolean over 1M rows -> ~900k). NDV must reflect the true distinct count.
TEST_F(OptimizerTest, NdvReflectsTrueDistinctCount) {
  Schema flags_schema({
      Column("id", TypeId::INTEGER),
      Column("flag", TypeId::INTEGER), // only two distinct values (0/1)
  });
  ASSERT_TRUE(catalog_->create_table("flags", flags_schema).ok());
  TableInfo *flags_info = catalog_->get_table("flags");
  ASSERT_NE(flags_info, nullptr);

  constexpr int kRows = 200;
  for (int i = 0; i < kRows; ++i) {
    std::vector<TupleValue> values = {TupleValue(i), TupleValue(i % 2)};
    Tuple tuple(values, flags_schema);
    RID rid;
    ASSERT_TRUE(flags_info->table_heap->insert_tuple(tuple, &rid).ok());
  }

  statistics_->collect_statistics(flags_info->oid);
  const auto *stats = statistics_->get_table_stats(flags_info->oid);
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->row_count, static_cast<size_t>(kRows));

  // id is unique -> NDV == kRows; flag has exactly 2 distinct values.
  EXPECT_DOUBLE_EQ(stats->columns.at(0).distinct_values,
                   static_cast<double>(kRows));
  EXPECT_DOUBLE_EQ(stats->columns.at(1).distinct_values, 2.0);
}

TEST_F(OptimizerTest, PredicateSelectivity) {
  auto eq_expr = std::make_unique<ComparisonExpression>(
      ComparisonType::EQUAL, std::make_unique<ColumnRefExpression>("id"),
      std::make_unique<ConstantExpression>(TupleValue(1)));

  double sel =
      statistics_->estimate_selectivity(table_info_->oid, eq_expr.get());
  EXPECT_DOUBLE_EQ(sel, Statistics::EQUALITY_SELECTIVITY);
}

// Builds `column CMP const`, binding the column to its schema index so the
// optimizer can resolve it (the binder does this at runtime).
static std::unique_ptr<ComparisonExpression>
make_cmp(ComparisonType cmp, const std::string &column, size_t column_index,
         TupleValue value) {
  auto col = std::make_unique<ColumnRefExpression>(column);
  col->set_column_index(column_index);
  return std::make_unique<ComparisonExpression>(
      cmp, std::move(col),
      std::make_unique<ConstantExpression>(std::move(value)));
}

// Issue #12: equality selectivity was a flat 0.01 constant that ignored the
// column's real distinct-value count. With stats, `col = const` must be 1/NDV.
TEST_F(OptimizerTest, EqualitySelectivityUsesNdv) {
  statistics_->collect_statistics(table_info_->oid);

  // id: 100 distinct -> 1/100; age: 50 distinct (20..69, each twice) -> 1/50.
  auto id_eq = make_cmp(ComparisonType::EQUAL, "id", 0, TupleValue(42));
  auto age_eq = make_cmp(ComparisonType::EQUAL, "age", 2, TupleValue(30));
  EXPECT_DOUBLE_EQ(
      statistics_->estimate_selectivity(table_info_->oid, id_eq.get()),
      1.0 / 100.0);
  EXPECT_DOUBLE_EQ(
      statistics_->estimate_selectivity(table_info_->oid, age_eq.get()),
      1.0 / 50.0);

  // Inequality is the complement of equality selectivity: 1 - 1/NDV.
  auto age_neq = make_cmp(ComparisonType::NOT_EQUAL, "age", 2, TupleValue(30));
  EXPECT_DOUBLE_EQ(
      statistics_->estimate_selectivity(table_info_->oid, age_neq.get()),
      1.0 - 1.0 / 50.0);
}

// Issue #12: logical connectives returned fixed constants and never recursed.
TEST_F(OptimizerTest, LogicalSelectivityRecursesIntoOperands) {
  statistics_->collect_statistics(table_info_->oid);

  // id = 42 (1/100) AND age = 30 (1/50) -> product under independence.
  auto conj = std::make_unique<LogicalExpression>(
      LogicalOpType::AND,
      make_cmp(ComparisonType::EQUAL, "id", 0, TupleValue(42)),
      make_cmp(ComparisonType::EQUAL, "age", 2, TupleValue(30)));
  EXPECT_DOUBLE_EQ(
      statistics_->estimate_selectivity(table_info_->oid, conj.get()),
      (1.0 / 100.0) * (1.0 / 50.0));

  // OR combines by inclusion-exclusion: sL + sR - sL*sR.
  auto disj = std::make_unique<LogicalExpression>(
      LogicalOpType::OR,
      make_cmp(ComparisonType::EQUAL, "id", 0, TupleValue(42)),
      make_cmp(ComparisonType::EQUAL, "age", 2, TupleValue(30)));
  double sl = 1.0 / 100.0;
  double sr = 1.0 / 50.0;
  EXPECT_DOUBLE_EQ(
      statistics_->estimate_selectivity(table_info_->oid, disj.get()),
      sl + sr - sl * sr);
}

// Issue #12: range selectivity must reflect the covered fraction of the
// column's [min,max] domain, not a flat constant.
TEST_F(OptimizerTest, RangeSelectivityFromMinMax) {
  statistics_->collect_statistics(table_info_->oid);
  // age domain is [20, 69] -> span of 50 integer values.
  const column_id_t age_col = 2;

  // age <= 24  -> {20..24} = 5/50.
  EXPECT_DOUBLE_EQ(
      statistics_->range_selectivity(table_info_->oid, age_col,
                                     std::nullopt, BPTreeKey{24}),
      5.0 / 50.0);
  // Full domain -> 1.0.
  EXPECT_DOUBLE_EQ(
      statistics_->range_selectivity(table_info_->oid, age_col, BPTreeKey{20},
                                     BPTreeKey{69}),
      1.0);
  // Entirely above the domain -> 0.0.
  EXPECT_DOUBLE_EQ(
      statistics_->range_selectivity(table_info_->oid, age_col, BPTreeKey{70},
                                     std::nullopt),
      0.0);
  // Without collected stats, falls back to the Selinger 1/3 default.
  EXPECT_DOUBLE_EQ(
      statistics_->range_selectivity(9999, age_col, std::nullopt,
                                     BPTreeKey{24}),
      Statistics::RANGE_SELECTIVITY);
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

// Minimal plan node reporting a DML type, used to exercise the cost model's
// default (INSERT/UPDATE/DELETE) branch, which has no concrete node class.
class ModifyTestNode : public PlanNode {
public:
  explicit ModifyTestNode(const Schema *schema)
      : PlanNode(PlanNodeType::INSERT), schema_(schema) {}
  [[nodiscard]] const Schema *output_schema() const override { return schema_; }

private:
  const Schema *schema_;
};

// Issue #12: the DML/default cost branch set cost = children_cost and then
// returned cost + children_cost, counting the child subtree twice.
TEST_F(OptimizerTest, DmlCostDoesNotDoubleCountChild) {
  auto child = std::make_unique<SeqScanPlanNode>(table_info_->oid,
                                                 &output_schema_, nullptr);
  double child_cost = cost_model_->estimate_cost(child.get());
  ASSERT_GT(child_cost, 0.0);

  auto modify = std::make_unique<ModifyTestNode>(&output_schema_);
  modify->add_child(std::move(child));
  double modify_cost = cost_model_->estimate_cost(modify.get());

  // Child counted exactly once (no intrinsic DML cost), not doubled.
  EXPECT_DOUBLE_EQ(modify_cost, child_cost);
}

// Issue #12: an index tuple fetch cost less than a sequential tuple (0.005 vs
// 0.01) and ignored random I/O. The marginal cost of fetching one matching
// tuple via the index must now exceed a plain sequential tuple.
TEST_F(OptimizerTest, IndexFetchCostsMoreThanSequentialTuple) {
  size_t rows = statistics_->table_cardinality(table_info_->oid);
  double seek = std::log2(static_cast<double>(rows)) *
                CostModel::RANDOM_PAGE_COST;

  // Index full scan touches every row via random fetches.
  auto index_full = std::make_unique<IndexScanPlanNode>(
      table_info_->oid, 1, &output_schema_,
      IndexScanPlanNode::ScanType::FULL_SCAN);
  double full_cost = cost_model_->estimate_cost(index_full.get());

  double marginal_per_tuple = (full_cost - seek) / static_cast<double>(rows);
  EXPECT_GT(marginal_per_tuple, CostModel::TUPLE_CPU_COST);
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

// Issue #12: filter cardinality was child_card * 0.1 for every predicate,
// ignoring the predicate entirely. It must vary with the predicate shape.
TEST_F(OptimizerTest, FilterCardinalityDependsOnPredicate) {
  auto make_filter = [&](std::unique_ptr<Expression> pred) {
    auto child = std::make_unique<SeqScanPlanNode>(table_info_->oid,
                                                   &output_schema_, nullptr);
    auto filter =
        std::make_unique<FilterPlanNode>(std::move(pred), &output_schema_);
    filter->add_child(std::move(child));
    return filter;
  };

  size_t base = cost_model_->estimate_cardinality(
      std::make_unique<SeqScanPlanNode>(table_info_->oid, &output_schema_,
                                        nullptr)
          .get());
  ASSERT_EQ(base, 100u);

  auto eq_filter =
      make_filter(make_cmp(ComparisonType::EQUAL, "id", 0, TupleValue(42)));
  auto range_filter =
      make_filter(make_cmp(ComparisonType::LESS_THAN, "age", 2, TupleValue(30)));

  size_t eq_card = cost_model_->estimate_cardinality(eq_filter.get());
  size_t range_card = cost_model_->estimate_cardinality(range_filter.get());

  // Equality (1/10 default) is more selective than a range (1/3 default), and
  // the range no longer collapses to the old flat 0.1 result (10).
  EXPECT_EQ(eq_card, static_cast<size_t>(static_cast<double>(base) *
                                         Statistics::EQUALITY_SELECTIVITY));
  EXPECT_EQ(range_card, static_cast<size_t>(static_cast<double>(base) *
                                            Statistics::RANGE_SELECTIVITY));
  EXPECT_LT(eq_card, range_card);
}

TEST_F(OptimizerTest, CardinalityEstimation) {
  auto seq_scan = std::make_unique<SeqScanPlanNode>(table_info_->oid,
                                                    &output_schema_, nullptr);

  size_t cardinality = cost_model_->estimate_cardinality(seq_scan.get());
  EXPECT_EQ(cardinality, 100);
}

} // namespace entropy
