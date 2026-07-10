/**
 * @file database_test.cpp
 * @brief Integration tests for Database class
 */

#include <gtest/gtest.h>

#include <string>

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

} // namespace
} // namespace entropy
