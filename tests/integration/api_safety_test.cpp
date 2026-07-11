/**
 * @file api_safety_test.cpp
 * @brief Regression tests for issue #26 API-safety nits.
 *
 * Two guarantees:
 *  - A moved-from Database is left in a valid, empty state: its noexcept
 *    accessors answer without dereferencing a null pimpl.
 *  - ENTROPY_ASSIGN_OR_RETURN expands against the real Result API and behaves
 *    correctly on both the ok and error paths.
 */

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "common/status.hpp"
#include "entropy/entropy.hpp"
#include "test_utils.hpp"

namespace entropy {
namespace {

class ApiSafetyTest : public ::testing::Test {
protected:
  void SetUp() override {
    temp_file_ = std::make_unique<test::TempFile>("api_safety_test_");
  }
  void TearDown() override { temp_file_.reset(); }

  std::unique_ptr<test::TempFile> temp_file_;
};

// A moved-from Database must not crash on its noexcept accessors; it reports an
// empty, closed state instead of dereferencing a moved-out pimpl.
TEST_F(ApiSafetyTest, MovedFromDatabaseAccessorsAreSafe) {
  Database source(temp_file_->string());
  ASSERT_TRUE(source.is_open());

  Database moved(std::move(source));
  ASSERT_TRUE(moved.is_open());

  // NOLINTBEGIN(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_FALSE(source.is_open());
  EXPECT_TRUE(source.path().empty());
  EXPECT_FALSE(source.in_transaction());
  EXPECT_EQ(source.catalog_for_testing(), nullptr);
  // NOLINTEND(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
}

// Same guarantee for move-assignment: the assigned-from source is emptied.
TEST_F(ApiSafetyTest, MoveAssignedFromDatabaseAccessorsAreSafe) {
  Database source(temp_file_->string());
  ASSERT_TRUE(source.is_open());

  test::TempFile other_file("api_safety_test_dst_");
  Database dest(other_file.string());
  ASSERT_TRUE(dest.is_open());

  dest = std::move(source);
  ASSERT_TRUE(dest.is_open());

  // NOLINTBEGIN(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_FALSE(source.is_open());
  EXPECT_TRUE(source.path().empty());
  EXPECT_FALSE(source.in_transaction());
  EXPECT_EQ(source.catalog_for_testing(), nullptr);
  // NOLINTEND(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
}

// Helper that exercises ENTROPY_ASSIGN_OR_RETURN: on a failed Result it returns
// the carried Status; on success it binds the result rows into `out`.
Status CollectRows(Database &db, std::string_view sql, std::vector<Row> &out) {
  ENTROPY_ASSIGN_OR_RETURN(out, db.execute(sql));
  return Status::Ok();
}

TEST_F(ApiSafetyTest, AssignOrReturnOkPathBindsRows) {
  Database db(temp_file_->string());
  ASSERT_TRUE(db.is_open());
  ASSERT_TRUE(db.execute("CREATE TABLE t (id INTEGER)").ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (1)").ok());
  ASSERT_TRUE(db.execute("INSERT INTO t VALUES (2)").ok());

  std::vector<Row> rows;
  Status status = CollectRows(db, "SELECT id FROM t", rows);

  EXPECT_TRUE(status.ok()) << status.to_string();
  EXPECT_EQ(rows.size(), 2U);
}

TEST_F(ApiSafetyTest, AssignOrReturnErrorPathReturnsStatus) {
  Database db(temp_file_->string());
  ASSERT_TRUE(db.is_open());

  std::vector<Row> rows;
  // Table does not exist, so execute() yields an error Result and the macro
  // must early-return that Status without touching `rows`.
  Status status = CollectRows(db, "SELECT id FROM missing_table", rows);

  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(rows.empty());
}

} // namespace
} // namespace entropy
