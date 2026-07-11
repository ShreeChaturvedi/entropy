/**
 * @file database_test.cpp
 * @brief Integration tests for Database class
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.hpp"
#include "entropy/entropy.hpp"
#include "test_utils.hpp"

namespace entropy {
namespace {

class DatabaseTest : public ::testing::Test {
protected:
  void SetUp() override {
    temp_file_ = std::make_unique<test::TempFile>("db_test_");
  }

  void TearDown() override { temp_file_.reset(); }

  std::unique_ptr<test::TempFile> temp_file_;
};

TEST_F(DatabaseTest, OpenClose) {
  Database db(temp_file_->string());

  EXPECT_TRUE(db.is_open());
  EXPECT_EQ(db.path(), temp_file_->string());

  db.close();
  EXPECT_FALSE(db.is_open());
}

// Regression (#17): if the on-disk file cannot be created/opened, the Database
// must not report is_open() == true or accept execute() calls that would only
// mutate an in-memory buffer pool with no persistent backing.
TEST_F(DatabaseTest, OpenFailureLeavesDatabaseClosed) {
  // Parent directory does not exist, so DiskManager cannot create the file.
  const std::string bad_path =
      temp_file_->string() + "/missing_parent/db.entropy";

  Database db(bad_path);
  EXPECT_FALSE(db.is_open());

  auto result = db.execute("CREATE TABLE t (id INTEGER)");
  EXPECT_FALSE(result.ok()) << result.status().to_string();
}

// Regression (#17): execute() after close() must error instead of mutating the
// still-live buffer pool. Post-close writes used to succeed in memory, then be
// silently lost because close()/destructor skip flush when is_open_ is false.
TEST_F(DatabaseTest, ExecuteAfterCloseErrors) {
  Database db(temp_file_->string());
  ASSERT_TRUE(db.is_open());

  auto result = db.execute("CREATE TABLE t (id INTEGER)");
  ASSERT_TRUE(result.ok()) << result.status().to_string();

  db.close();
  EXPECT_FALSE(db.is_open());

  result = db.execute("INSERT INTO t VALUES (1)");
  EXPECT_FALSE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.status().code(), StatusCode::kInvalidArgument);
}

TEST_F(DatabaseTest, Transaction) {
  Database db(temp_file_->string());

  EXPECT_FALSE(db.in_transaction());

  auto status = db.begin_transaction();
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(db.in_transaction());

  status = db.commit();
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(db.in_transaction());
}

TEST_F(DatabaseTest, NestedTransaction) {
  Database db(temp_file_->string());

  auto status = db.begin_transaction();
  EXPECT_TRUE(status.ok());

  // Nested transaction should fail
  status = db.begin_transaction();
  EXPECT_FALSE(status.ok());

  status = db.rollback();
  EXPECT_TRUE(status.ok());
}

TEST_F(DatabaseTest, CommitWithoutTransaction) {
  Database db(temp_file_->string());

  auto status = db.commit();
  EXPECT_FALSE(status.ok());
}

TEST_F(DatabaseTest, RollbackWithoutTransaction) {
  Database db(temp_file_->string());

  auto status = db.rollback();
  EXPECT_FALSE(status.ok());
}

TEST_F(DatabaseTest, Version) {
  EXPECT_STREQ(version(), "0.1.1");
  EXPECT_EQ(version_major(), 0);
  EXPECT_EQ(version_minor(), 1);
  EXPECT_EQ(version_patch(), 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// SQL Execution Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST_F(DatabaseTest, CreateTable) {
  Database db(temp_file_->string());

  auto result =
      db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100))");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
}

TEST_F(DatabaseTest, CreateDuplicateTable) {
  Database db(temp_file_->string());

  auto result = db.execute("CREATE TABLE users (id INTEGER)");
  EXPECT_TRUE(result.ok());

  result = db.execute("CREATE TABLE users (id INTEGER)");
  EXPECT_FALSE(result.ok());
}

TEST_F(DatabaseTest, InsertAndSelect) {
  Database db(temp_file_->string());

  // Create table
  auto result =
      db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100))");
  EXPECT_TRUE(result.ok()) << result.status().to_string();

  // Insert data
  result = db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.affected_rows(), 1);

  // Select all
  result = db.execute("SELECT * FROM users");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.row_count(), 1);
  EXPECT_EQ(result.column_names().size(), 2);
}

TEST_F(DatabaseTest, SelectWithWhere) {
  Database db(temp_file_->string());

  auto result =
      db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100), age INTEGER)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  result = db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  result = db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  result = db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();

  result = db.execute("SELECT * FROM users WHERE age > 28");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.row_count(), 2); // Bob and Charlie
}

TEST_F(DatabaseTest, SelectColumns) {
  Database db(temp_file_->string());

  auto result =
      db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100), age INTEGER)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  result = db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();

  result = db.execute("SELECT name, age FROM users");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.column_names().size(), 2);
  EXPECT_EQ(result.column_names()[0], "name");
  EXPECT_EQ(result.column_names()[1], "age");
}

TEST_F(DatabaseTest, UpdateRows) {
  Database db(temp_file_->string());

  auto result =
      db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100), age INTEGER)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  result = db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  result = db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();

  result = db.execute("UPDATE users SET age = 99 WHERE id = 1");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.affected_rows(), 1);

  // Verify update
  result = db.execute("SELECT * FROM users WHERE id = 1");
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.row_count(), 1);
}

TEST_F(DatabaseTest, DeleteRows) {
  Database db(temp_file_->string());

  auto result = db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100))");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  result = db.execute("INSERT INTO users VALUES (1, 'Alice')");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  result = db.execute("INSERT INTO users VALUES (2, 'Bob')");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  result = db.execute("INSERT INTO users VALUES (3, 'Charlie')");
  EXPECT_TRUE(result.ok()) << result.status().to_string();

  result = db.execute("DELETE FROM users WHERE id = 2");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.affected_rows(), 1);

  // Verify deletion
  result = db.execute("SELECT * FROM users");
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.row_count(), 2);
}

TEST_F(DatabaseTest, DropTable) {
  Database db(temp_file_->string());

  auto result = db.execute("CREATE TABLE temp (id INTEGER)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  result = db.execute("DROP TABLE temp");
  EXPECT_TRUE(result.ok()) << result.status().to_string();

  // Should fail - table dropped
  result = db.execute("SELECT * FROM temp");
  EXPECT_FALSE(result.ok());
}

TEST_F(DatabaseTest, SelectNonexistentTable) {
  Database db(temp_file_->string());

  auto result = db.execute("SELECT * FROM nonexistent");
  EXPECT_FALSE(result.ok());
}

TEST_F(DatabaseTest, InsertMultipleRows) {
  Database db(temp_file_->string());

  auto result = db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100))");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  result = db.execute(
      "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.affected_rows(), 3);

  result = db.execute("SELECT * FROM users");
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.row_count(), 3);
}

TEST_F(DatabaseTest, OrderByNonProjectedColumn) {
  Database db(temp_file_->string());

  auto result = db.execute("CREATE TABLE t (name VARCHAR(20), age INTEGER)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_TRUE(db.execute("INSERT INTO t VALUES ('Alice', 30)").ok());
  EXPECT_TRUE(db.execute("INSERT INTO t VALUES ('Bob', 10)").ok());
  EXPECT_TRUE(db.execute("INSERT INTO t VALUES ('Carol', 20)").ok());

  // Regression (#21): ORDER BY on a column absent from the SELECT list must
  // still sort the output rather than being silently dropped.
  result = db.execute("SELECT name FROM t ORDER BY age");
  ASSERT_TRUE(result.ok()) << result.status().to_string();
  ASSERT_EQ(result.row_count(), 3);
  ASSERT_EQ(result.column_names().size(), 1);
  EXPECT_EQ(result.column_names()[0], "name");
  // Ordered by age ascending: Bob (10), Carol (20), Alice (30).
  EXPECT_EQ(result.rows()[0][0].as_string(), "Bob");
  EXPECT_EQ(result.rows()[1][0].as_string(), "Carol");
  EXPECT_EQ(result.rows()[2][0].as_string(), "Alice");

  // Descending on a non-projected column too.
  result = db.execute("SELECT name FROM t ORDER BY age DESC");
  ASSERT_TRUE(result.ok()) << result.status().to_string();
  ASSERT_EQ(result.row_count(), 3);
  EXPECT_EQ(result.rows()[0][0].as_string(), "Alice");
  EXPECT_EQ(result.rows()[1][0].as_string(), "Carol");
  EXPECT_EQ(result.rows()[2][0].as_string(), "Bob");
}

TEST_F(DatabaseTest, OrderByUnknownColumnErrors) {
  Database db(temp_file_->string());

  auto result = db.execute("CREATE TABLE t (name VARCHAR(20), age INTEGER)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_TRUE(db.execute("INSERT INTO t VALUES ('Alice', 30)").ok());

  // ORDER BY a column that does not exist must be a clear error, not a
  // silently-unsorted result.
  result = db.execute("SELECT name FROM t ORDER BY nonexistent");
  EXPECT_FALSE(result.ok());
}

// Regression test for #24: a SELECT that legitimately matches zero rows
// must still be distinguishable from a DML result, so the shell can render
// column headers instead of an "affected rows" message.
TEST_F(DatabaseTest, EmptySelectIsDistinguishableFromDml) {
  Database db(temp_file_->string());

  auto result =
      db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100))");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  result = db.execute("INSERT INTO users VALUES (1, 'Alice')");
  EXPECT_TRUE(result.ok()) << result.status().to_string();

  // A SELECT that matches zero rows is still a query: is_query() is true,
  // column names are still populated, and it has no rows.
  result = db.execute("SELECT * FROM users WHERE 1 = 0");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_TRUE(result.is_query());
  EXPECT_FALSE(result.has_rows());
  EXPECT_EQ(result.row_count(), 0);
  EXPECT_EQ(result.column_names().size(), 2);

  // A DML statement that affects zero rows is not a query: is_query() is
  // false even though it also has no rows.
  result = db.execute("UPDATE users SET name = 'Bob' WHERE id = 999");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_FALSE(result.is_query());
  EXPECT_FALSE(result.has_rows());
  EXPECT_EQ(result.affected_rows(), 0);

  // A non-empty SELECT is still a query, of course.
  result = db.execute("SELECT * FROM users");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_TRUE(result.is_query());
  EXPECT_TRUE(result.has_rows());
}

// Regression test for #9: SELECT of narrow numeric columns (SMALLINT/TINYINT/
// FLOAT) must return the stored value, not a silent NULL. Before the fix,
// tuple_value_to_value handled only int32/int64/double and these fell through
// to Value() (NULL).
TEST_F(DatabaseTest, SelectNarrowNumericTypes) {
  Database db(temp_file_->string());

  auto result = db.execute(
      "CREATE TABLE nums (s SMALLINT, t TINYINT, f FLOAT)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();

  result = db.execute("INSERT INTO nums VALUES (5, 7, 2.5)");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.affected_rows(), 1);

  result = db.execute("SELECT s, t, f FROM nums");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  ASSERT_EQ(result.row_count(), 1);

  const auto &row = result.rows()[0];
  ASSERT_EQ(row.size(), 3u);

  // None of these may be NULL (the bug returned NULL for all three).
  EXPECT_FALSE(row[0].is_null()) << "SMALLINT column returned NULL";
  EXPECT_FALSE(row[1].is_null()) << "TINYINT column returned NULL";
  EXPECT_FALSE(row[2].is_null()) << "FLOAT column returned NULL";

  // Narrow integers widen to int32, FLOAT widens to double.
  EXPECT_EQ(row[0].as_int32(), 5);
  EXPECT_EQ(row[1].as_int32(), 7);
  EXPECT_DOUBLE_EQ(row[2].as_double(), 2.5);
}

// End-to-end regression test for #4: DatabaseImpl::execute_select must resolve
// the optimizer-chosen index by OID (get_index_by_oid), never by casting the
// index OID to a column_id_t and calling get_index_for_column. This drives the
// real SELECT path (parse -> bind -> optimize -> index scan). Reverting the
// one-line fix in execute_select makes this test fail: the point lookup then
// resolves to the wrong index and returns the wrong row.
TEST_F(DatabaseTest, IndexScanResolvesIndexByOidEndToEnd) {
  Database db(temp_file_->string());

  // Three columns so an index OID can numerically alias a column position:
  // a=col0, pad=col1, c=col2.
  ASSERT_TRUE(
      db.execute("CREATE TABLE t (a INTEGER, pad INTEGER, c INTEGER)").ok());

  // Enough rows that the optimizer prefers an index scan over a seq scan for a
  // point lookup. a_i = i is unique; c_i = N+1-i is a reversal (also unique and
  // never equal to a_i for even N), so the row with a=V and the row with c=V
  // are distinct rows.
  constexpr int kN = 2000;
  constexpr int kV = 42;
  for (int base = 1; base <= kN; base += 200) {
    std::string sql = "INSERT INTO t VALUES ";
    for (int i = base; i < base + 200 && i <= kN; ++i) {
      if (i != base) {
        sql += ", ";
      }
      sql += "(" + std::to_string(i) + ", " + std::to_string(i) + ", " +
             std::to_string(kN + 1 - i) + ")";
    }
    ASSERT_TRUE(db.execute(sql).ok()) << "insert batch starting at " << base;
  }

  // Create the two indexes through the test seam (CREATE INDEX is not yet
  // parseable). Order reproduces the bug: table oid=1, idx_a gets oid=2
  // (key_column 0), idx_c gets oid=3 (key_column 2). Casting idx_a's OID (2) to
  // a column_id selects the index whose key_column==2 — idx_c — the wrong one.
  Catalog *catalog = db.catalog_for_testing();
  ASSERT_NE(catalog, nullptr);
  ASSERT_TRUE(catalog->create_index("idx_a", "t", "a").ok());
  ASSERT_TRUE(catalog->create_index("idx_c", "t", "c").ok());

  IndexInfo *idx_a = catalog->get_index("idx_a");
  IndexInfo *idx_c = catalog->get_index("idx_c");
  ASSERT_NE(idx_a, nullptr);
  ASSERT_NE(idx_c, nullptr);
  ASSERT_NE(idx_a->oid, idx_c->oid);
  // Precondition for the bug to bite: idx_a's OID, cast to a column id, lands on
  // idx_c's key column. If this ever stops holding, the test no longer guards
  // the regression and should be revisited.
  ASSERT_EQ(static_cast<column_id_t>(idx_a->oid), idx_c->key_column)
      << "test no longer reproduces the OID/column_id aliasing";

  // The optimizer must actually choose an index scan here, otherwise the SELECT
  // below would never reach the index-resolution line.
  auto explain = db.execute("EXPLAIN SELECT * FROM t WHERE a = 42");
  ASSERT_TRUE(explain.ok()) << explain.status().to_string();
  bool used_index = false;
  for (const auto &r : explain.rows()) {
    if (r[0].as_string().find("Index Scan") != std::string::npos) {
      used_index = true;
    }
  }
  ASSERT_TRUE(used_index) << "optimizer did not choose an index scan";

  // The real query. Correct resolution (get_index_by_oid) uses idx_a and
  // returns exactly the row with a=42. The old bug resolves to idx_c and does a
  // point lookup for c=42, returning the row (a=1959, c=42) instead.
  auto result = db.execute("SELECT * FROM t WHERE a = 42");
  ASSERT_TRUE(result.ok()) << result.status().to_string();
  ASSERT_EQ(result.row_count(), 1u);
  const auto &row = result.rows()[0];
  ASSERT_EQ(row.size(), 3u);
  EXPECT_EQ(row[0].as_int32(), kV)
      << "wrong index resolved: OID was cast to a column_id";
  EXPECT_EQ(row[2].as_int32(), kN + 1 - kV);
}

// ─────────────────────────────────────────────────────────────────────────────
// Query reachability (#16): joins, aggregation, GROUP BY, and computed/aliased
// projections must run end-to-end through Database::execute().
// ─────────────────────────────────────────────────────────────────────────────

// A two-table inner equi-join returns exactly the matching (left, right) rows.
TEST_F(DatabaseTest, TwoTableInnerJoinReturnsCorrectRows) {
  Database db(temp_file_->string());
  ASSERT_TRUE(db.execute("CREATE TABLE emp (id INTEGER, dept INTEGER)").ok());
  ASSERT_TRUE(
      db.execute("CREATE TABLE dept (did INTEGER, name VARCHAR)").ok());
  ASSERT_TRUE(
      db.execute("INSERT INTO emp VALUES (1,10),(2,10),(3,20),(4,30)").ok());
  ASSERT_TRUE(
      db.execute("INSERT INTO dept VALUES (10,'Eng'),(20,'Sales')").ok());

  // emp row 4 (dept 30) has no matching department and must be dropped by the
  // inner join.
  auto result = db.execute(
      "SELECT emp.id AS id, dept.name AS name "
      "FROM emp JOIN dept ON emp.dept = dept.did");
  ASSERT_TRUE(result.ok()) << result.status().to_string();

  std::vector<std::pair<int, std::string>> got;
  for (const auto &row : result.rows()) {
    got.emplace_back(row["id"].as_int32(), row["name"].as_string());
  }
  std::sort(got.begin(), got.end());

  const std::vector<std::pair<int, std::string>> want = {
      {1, "Eng"}, {2, "Eng"}, {3, "Sales"}};
  EXPECT_EQ(got, want);
}

// GROUP BY with every supported aggregate produces the correct per-group value.
TEST_F(DatabaseTest, GroupByAggregatesAreCorrect) {
  Database db(temp_file_->string());
  ASSERT_TRUE(db.execute("CREATE TABLE emp (dept INTEGER, sal INTEGER)").ok());
  ASSERT_TRUE(
      db.execute("INSERT INTO emp VALUES (10,100),(10,200),(20,300),(20,50),"
                 "(20,120)")
          .ok());

  auto result = db.execute(
      "SELECT dept, COUNT(*) AS c, SUM(sal) AS s, MIN(sal) AS mn, "
      "MAX(sal) AS mx, AVG(sal) AS av FROM emp GROUP BY dept ORDER BY dept");
  ASSERT_TRUE(result.ok()) << result.status().to_string();
  ASSERT_EQ(result.row_count(), 2u);

  // ORDER BY dept: group 10 first, then 20.
  const auto &g10 = result.rows()[0];
  EXPECT_EQ(g10["dept"].as_int32(), 10);
  EXPECT_EQ(g10["c"].as_int64(), 2);
  EXPECT_EQ(g10["s"].as_int64(), 300);
  EXPECT_EQ(g10["mn"].as_int32(), 100);
  EXPECT_EQ(g10["mx"].as_int32(), 200);
  EXPECT_DOUBLE_EQ(g10["av"].as_double(), 150.0);

  const auto &g20 = result.rows()[1];
  EXPECT_EQ(g20["dept"].as_int32(), 20);
  EXPECT_EQ(g20["c"].as_int64(), 3);
  EXPECT_EQ(g20["s"].as_int64(), 470);
  EXPECT_EQ(g20["mn"].as_int32(), 50);
  EXPECT_EQ(g20["mx"].as_int32(), 300);
  EXPECT_NEAR(g20["av"].as_double(), 470.0 / 3.0, 1e-9);
}

// COUNT(*) with no GROUP BY aggregates the whole table into a single row.
TEST_F(DatabaseTest, CountStarWholeTable) {
  Database db(temp_file_->string());
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER)").ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (1),(2),(3),(4)").ok());

  auto result = db.execute("SELECT COUNT(*) AS n FROM t");
  ASSERT_TRUE(result.ok()) << result.status().to_string();
  ASSERT_EQ(result.row_count(), 1u);
  EXPECT_EQ(result.rows()[0]["n"].as_int64(), 4);
}

// Computed arithmetic expressions and aliases produce correctly named, typed
// output columns.
TEST_F(DatabaseTest, ComputedAndAliasedProjection) {
  Database db(temp_file_->string());
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER, sal INTEGER)").ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (1,100),(2,250)").ok());

  auto result = db.execute(
      "SELECT id AS emp_id, sal + 10 AS bonus, sal * 2 AS doubled FROM t "
      "ORDER BY emp_id");
  ASSERT_TRUE(result.ok()) << result.status().to_string();
  ASSERT_EQ(result.row_count(), 2u);

  const std::vector<std::string> expected_cols = {"emp_id", "bonus", "doubled"};
  EXPECT_EQ(result.column_names(), expected_cols);

  // Arithmetic over integers widens to BIGINT (int64) in this engine.
  const auto &r0 = result.rows()[0];
  EXPECT_EQ(r0["emp_id"].as_int32(), 1);
  EXPECT_EQ(r0["bonus"].as_int64(), 110);
  EXPECT_EQ(r0["doubled"].as_int64(), 200);

  const auto &r1 = result.rows()[1];
  EXPECT_EQ(r1["emp_id"].as_int32(), 2);
  EXPECT_EQ(r1["bonus"].as_int64(), 260);
  EXPECT_EQ(r1["doubled"].as_int64(), 500);
}

// Robustness: an ill-typed but parseable computed or aggregate expression (e.g.
// arithmetic or SUM over a VARCHAR column) must surface a clean error Status
// through the public API, never abort the host process. Expression evaluation
// can throw std::bad_variant_access deep in the executor drain; that exception
// must be caught and converted, not allowed to escape execute().
TEST_F(DatabaseTest, IllTypedExpressionReturnsErrorNotCrash) {
  Database db(temp_file_->string());
  ASSERT_TRUE(
      db.execute("CREATE TABLE t (id INTEGER, name VARCHAR(100))").ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (1,'Alice'),(2,'Bob')").ok());

  // Each of these is parseable but ill-typed. None may abort the process; every
  // one must come back as an error Status.
  const std::vector<std::string> ill_typed = {
      "SELECT name + 1 FROM t",
      "SELECT sal + name FROM t",
      "SELECT name * 2 FROM t",
      "SELECT SUM(name) FROM t",
      "SELECT id, name + 1 FROM t ORDER BY id",
  };

  for (const auto &sql : ill_typed) {
    auto result = db.execute(sql);
    EXPECT_FALSE(result.ok()) << "expected error for: " << sql;
  }

  // The database is still usable after the ill-typed queries: a well-typed
  // query returns correct rows.
  auto ok = db.execute("SELECT id, name FROM t ORDER BY id");
  ASSERT_TRUE(ok.ok()) << ok.status().to_string();
  ASSERT_EQ(ok.row_count(), 2u);
  EXPECT_EQ(ok.rows()[0]["name"].as_string(), "Alice");
  EXPECT_EQ(ok.rows()[1]["name"].as_string(), "Bob");
}

// Robustness (companion to the SELECT case above): the same
// std::bad_variant_access crash class is reachable through DML. execute_update
// and execute_insert drain their executors outside collect_result's SELECT
// guard, so before the dispatch-level guard an ill-typed UPDATE/INSERT threw
// straight out of the public execute() and aborted the host. Each must now come
// back as a clean error Status, and the database must stay usable afterward.
TEST_F(DatabaseTest, IllTypedDmlReturnsErrorNotCrash) {
  Database db(temp_file_->string());
  ASSERT_TRUE(
      db.execute("CREATE TABLE emp (id INTEGER, name VARCHAR(50), sal INTEGER)")
          .ok());
  ASSERT_TRUE(
      db.execute("INSERT INTO emp VALUES (1,'Alice',100),(2,'Bob',200)").ok());

  // An ill-typed UPDATE SET value or INSERT literal throws when the mistyped
  // result is materialized into the target column. None may abort the process;
  // every one must come back as an error Status.
  const std::vector<std::string> ill_typed_dml = {
      "UPDATE emp SET sal = name + 1 WHERE id = 1",
      "UPDATE emp SET sal = name * sal WHERE id = 2",
      "INSERT INTO emp VALUES (3, 'Carol', 'not-an-int')",
  };
  for (const auto &sql : ill_typed_dml) {
    auto result = db.execute(sql);
    EXPECT_FALSE(result.ok()) << "expected error for: " << sql;
  }

  // An ill-typed DELETE can't reach that throw: its only expression is the
  // WHERE predicate, which evaluates to a fresh boolean, never a materialized
  // mistyped column, so it matches nothing. It must still not crash.
  auto del = db.execute("DELETE FROM emp WHERE name * 2 = 4");
  EXPECT_TRUE(del.ok()) << del.status().to_string();
  EXPECT_EQ(del.affected_rows(), 0u);

  // The database is still usable after the ill-typed DML: the two original rows
  // are intact (the aborted statements applied nothing) and a well-typed UPDATE
  // commits. sal is INTEGER, so it reads back as int32.
  auto after = db.execute("SELECT id, sal FROM emp ORDER BY id");
  ASSERT_TRUE(after.ok()) << after.status().to_string();
  ASSERT_EQ(after.row_count(), 2u);
  EXPECT_EQ(after.rows()[0]["sal"].as_int32(), 100);
  EXPECT_EQ(after.rows()[1]["sal"].as_int32(), 200);

  auto upd = db.execute("UPDATE emp SET sal = sal + 1 WHERE id = 1");
  ASSERT_TRUE(upd.ok()) << upd.status().to_string();
  EXPECT_EQ(upd.affected_rows(), 1u);
}

// #10: on a large indexed table the optimizer picks an index scan for a
// selective point lookup and a sequential scan when there is no predicate,
// observable through EXPLAIN's rendered plan.
TEST_F(DatabaseTest, OptimizerChoosesIndexVsSeqScan) {
  Database db(temp_file_->string());
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER, v INTEGER)").ok());

  // Enough rows that a point lookup via the index beats a full scan.
  constexpr int kN = 3000;
  for (int base = 0; base < kN; base += 500) {
    std::string sql = "INSERT INTO t VALUES ";
    for (int i = base; i < base + 500 && i < kN; ++i) {
      if (i != base) {
        sql += ", ";
      }
      sql += "(" + std::to_string(i) + ", " + std::to_string(i % 5) + ")";
    }
    ASSERT_TRUE(db.execute(sql).ok()) << "insert batch at " << base;
  }
  ASSERT_TRUE(db.catalog_for_testing()->create_index("idx_t_id", "t", "id").ok());

  auto explain_contains = [&](const std::string &sql, const std::string &needle) {
    auto r = db.execute(sql);
    EXPECT_TRUE(r.ok()) << r.status().to_string();
    for (const auto &row : r.rows()) {
      if (row[0].as_string().find(needle) != std::string::npos) {
        return true;
      }
    }
    return false;
  };

  // Selective equality -> index scan.
  EXPECT_TRUE(explain_contains("EXPLAIN SELECT * FROM t WHERE id = 5",
                               "Index Scan"));
  // No predicate -> sequential scan.
  EXPECT_TRUE(
      explain_contains("EXPLAIN SELECT * FROM t", "Sequential Scan"));

  // The index-backed query still returns the correct row.
  auto row = db.execute("SELECT id FROM t WHERE id = 5");
  ASSERT_TRUE(row.ok()) << row.status().to_string();
  ASSERT_EQ(row.row_count(), 1u);
  EXPECT_EQ(row.rows()[0]["id"].as_int32(), 5);
}

} // namespace
} // namespace entropy
