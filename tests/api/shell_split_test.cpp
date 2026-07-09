/**
 * @file shell_split_test.cpp
 * @brief Tests for string-literal-aware SQL statement termination (issue #32)
 */

#include <gtest/gtest.h>

#include "api/shell_utils.hpp"

namespace entropy {
namespace {

TEST(ShellSplitTest, SemicolonOutsideStringCompletes) {
  EXPECT_TRUE(sql_has_complete_statement("SELECT * FROM t;"));
  EXPECT_TRUE(sql_has_complete_statement("SELECT * FROM t WHERE id = 1;"));
}

TEST(ShellSplitTest, SemicolonInsideSingleQuotedStringDoesNotComplete) {
  EXPECT_FALSE(sql_has_complete_statement("SELECT * FROM t WHERE name = 'a;b'"));
  EXPECT_FALSE(
      sql_has_complete_statement("INSERT INTO t VALUES ('hello;world')"));
}

TEST(ShellSplitTest, EscapedQuoteDoesNotEndString) {
  // SQL '' escape inside a string; the semicolon is still inside the literal.
  EXPECT_FALSE(
      sql_has_complete_statement("SELECT * FROM t WHERE name = 'a'';b'"));
  EXPECT_TRUE(
      sql_has_complete_statement("SELECT * FROM t WHERE name = 'a'';b';"));
}

TEST(ShellSplitTest, CompletesAfterLiteralContainingSemicolon) {
  EXPECT_TRUE(
      sql_has_complete_statement("SELECT * FROM t WHERE name = 'a;b';"));
}

TEST(ShellSplitTest, EmptyAndNoSemicolon) {
  EXPECT_FALSE(sql_has_complete_statement(""));
  EXPECT_FALSE(sql_has_complete_statement("SELECT * FROM t"));
}

} // namespace
} // namespace entropy
