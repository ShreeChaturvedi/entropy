/**
 * @file database_test.cpp
 * @brief Integration tests for Database class
 */

#include <gtest/gtest.h>

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

  db.rollback();
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
  EXPECT_STREQ(version(), "0.1.0");
  EXPECT_EQ(version_major(), 0);
  EXPECT_EQ(version_minor(), 1);
  EXPECT_EQ(version_patch(), 0);
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

  db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100), age INTEGER)");
  db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
  db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
  db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");

  auto result = db.execute("SELECT * FROM users WHERE age > 28");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.row_count(), 2); // Bob and Charlie
}

TEST_F(DatabaseTest, SelectColumns) {
  Database db(temp_file_->string());

  db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100), age INTEGER)");
  db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");

  auto result = db.execute("SELECT name, age FROM users");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.column_names().size(), 2);
  EXPECT_EQ(result.column_names()[0], "name");
  EXPECT_EQ(result.column_names()[1], "age");
}

TEST_F(DatabaseTest, UpdateRows) {
  Database db(temp_file_->string());

  db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100), age INTEGER)");
  db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
  db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");

  auto result = db.execute("UPDATE users SET age = 99 WHERE id = 1");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.affected_rows(), 1);

  // Verify update
  result = db.execute("SELECT * FROM users WHERE id = 1");
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.row_count(), 1);
}

TEST_F(DatabaseTest, DeleteRows) {
  Database db(temp_file_->string());

  db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100))");
  db.execute("INSERT INTO users VALUES (1, 'Alice')");
  db.execute("INSERT INTO users VALUES (2, 'Bob')");
  db.execute("INSERT INTO users VALUES (3, 'Charlie')");

  auto result = db.execute("DELETE FROM users WHERE id = 2");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.affected_rows(), 1);

  // Verify deletion
  result = db.execute("SELECT * FROM users");
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.row_count(), 2);
}

TEST_F(DatabaseTest, DropTable) {
  Database db(temp_file_->string());

  db.execute("CREATE TABLE temp (id INTEGER)");
  auto result = db.execute("DROP TABLE temp");
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

  db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100))");
  auto result = db.execute(
      "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
  EXPECT_TRUE(result.ok()) << result.status().to_string();
  EXPECT_EQ(result.affected_rows(), 3);

  result = db.execute("SELECT * FROM users");
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.row_count(), 3);
}

} // namespace
} // namespace entropy
