/**
 * @file catalog_test.cpp
 * @brief Unit tests for Catalog
 */

#include <gtest/gtest.h>

#include "catalog/catalog.hpp"
#include "storage/buffer_pool.hpp"
#include "test_utils.hpp"

namespace entropy {
namespace {

class CatalogTest : public ::testing::Test {
protected:
    void SetUp() override {
        temp_file_ = std::make_unique<test::TempFile>("cat_test_");
        disk_manager_ = std::make_shared<DiskManager>(temp_file_->string());
        buffer_pool_ = std::make_shared<BufferPoolManager>(10, disk_manager_);
        catalog_ = std::make_unique<Catalog>(buffer_pool_);
    }

    void TearDown() override {
        catalog_.reset();
        buffer_pool_.reset();
        disk_manager_.reset();
        temp_file_.reset();
    }

    std::unique_ptr<test::TempFile> temp_file_;
    std::shared_ptr<DiskManager> disk_manager_;
    std::shared_ptr<BufferPoolManager> buffer_pool_;
    std::unique_ptr<Catalog> catalog_;
};

TEST_F(CatalogTest, CreateTable) {
    Schema schema({
        Column("id", TypeId::INTEGER),
        Column("name", TypeId::VARCHAR, 100),
    });

    auto status = catalog_->create_table("users", schema);
    EXPECT_TRUE(status.ok()) << status.to_string();

    EXPECT_TRUE(catalog_->table_exists("users"));
    EXPECT_FALSE(catalog_->table_exists("nonexistent"));
}

TEST_F(CatalogTest, DuplicateTable) {
    Schema schema({Column("id", TypeId::INTEGER)});

    auto status = catalog_->create_table("test", schema);
    EXPECT_TRUE(status.ok());

    status = catalog_->create_table("test", schema);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kAlreadyExists);
}

TEST_F(CatalogTest, GetTableSchema) {
    Schema schema({
        Column("id", TypeId::INTEGER),
        Column("value", TypeId::DOUBLE),
    });

    catalog_->create_table("data", schema);

    const Schema* retrieved = catalog_->get_table_schema("data");
    ASSERT_NE(retrieved, nullptr);
    EXPECT_EQ(retrieved->column_count(), 2);
    EXPECT_EQ(retrieved->column(0).name(), "id");
    EXPECT_EQ(retrieved->column(1).name(), "value");
}

TEST_F(CatalogTest, DropTable) {
    Schema schema({Column("id", TypeId::INTEGER)});
    catalog_->create_table("temp", schema);

    EXPECT_TRUE(catalog_->table_exists("temp"));

    auto status = catalog_->drop_table("temp");
    EXPECT_TRUE(status.ok());

    EXPECT_FALSE(catalog_->table_exists("temp"));
}

TEST_F(CatalogTest, DropNonexistentTable) {
    auto status = catalog_->drop_table("nonexistent");
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kNotFound);
}

}  // namespace
}  // namespace entropy
