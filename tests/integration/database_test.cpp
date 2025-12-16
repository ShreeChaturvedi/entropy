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

    void TearDown() override {
        temp_file_.reset();
    }

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

}  // namespace
}  // namespace entropy
