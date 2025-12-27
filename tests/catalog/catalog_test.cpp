/**
 * @file catalog_test.cpp
 * @brief Unit tests for Catalog
 */

#include <gtest/gtest.h>
#include <set>

#include "catalog/catalog.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/table_heap.hpp"
#include "storage/tuple.hpp"
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

  auto status = catalog_->create_table("data", schema);
  EXPECT_TRUE(status.ok());

  const Schema *retrieved = catalog_->get_table_schema("data");
  ASSERT_NE(retrieved, nullptr);
  EXPECT_EQ(retrieved->column_count(), 2);
  EXPECT_EQ(retrieved->column(0).name(), "id");
  EXPECT_EQ(retrieved->column(1).name(), "value");
}

TEST_F(CatalogTest, DropTable) {
  Schema schema({Column("id", TypeId::INTEGER)});
  auto status = catalog_->create_table("temp", schema);
  EXPECT_TRUE(status.ok());

  EXPECT_TRUE(catalog_->table_exists("temp"));

  status = catalog_->drop_table("temp");
  EXPECT_TRUE(status.ok());

  EXPECT_FALSE(catalog_->table_exists("temp"));
}

TEST_F(CatalogTest, DropNonexistentTable) {
  auto status = catalog_->drop_table("nonexistent");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), StatusCode::kNotFound);
}

TEST_F(CatalogTest, GetTableInfo) {
  Schema schema({
      Column("id", TypeId::INTEGER),
      Column("name", TypeId::VARCHAR, 100),
  });

  ASSERT_TRUE(catalog_->create_table("users", schema).ok());

  TableInfo *info = catalog_->get_table("users");
  ASSERT_NE(info, nullptr);
  EXPECT_EQ(info->name, "users");
  EXPECT_NE(info->oid, INVALID_OID);
  EXPECT_EQ(info->schema.column_count(), 2);
  EXPECT_NE(info->table_heap, nullptr);
}

TEST_F(CatalogTest, TableHeapIntegration) {
  Schema schema({
      Column("id", TypeId::INTEGER),
      Column("value", TypeId::VARCHAR, 50),
  });

  ASSERT_TRUE(catalog_->create_table("data", schema).ok());

  TableInfo *info = catalog_->get_table("data");
  ASSERT_NE(info, nullptr);
  ASSERT_NE(info->table_heap, nullptr);

  // Insert a tuple using the TableHeap from catalog
  std::vector<TupleValue> values = {TupleValue(42),
                                    TupleValue(std::string("hello"))};
  Tuple tuple(values, schema);

  RID rid;
  EXPECT_TRUE(info->table_heap->insert_tuple(tuple, &rid).ok());

  // Read it back
  Tuple read_tuple;
  EXPECT_TRUE(info->table_heap->get_tuple(rid, &read_tuple).ok());
  EXPECT_EQ(read_tuple.get_value(schema, 0).as_integer(), 42);
  EXPECT_EQ(read_tuple.get_value(schema, 1).as_string(), "hello");
}

TEST_F(CatalogTest, GetTableNames) {
  Schema schema({Column("id", TypeId::INTEGER)});

  ASSERT_TRUE(catalog_->create_table("table1", schema).ok());
  ASSERT_TRUE(catalog_->create_table("table2", schema).ok());
  ASSERT_TRUE(catalog_->create_table("table3", schema).ok());

  auto names = catalog_->get_table_names();
  EXPECT_EQ(names.size(), 3);

  // Check all three are present (order not guaranteed)
  std::set<std::string> name_set(names.begin(), names.end());
  EXPECT_TRUE(name_set.count("table1") > 0);
  EXPECT_TRUE(name_set.count("table2") > 0);
  EXPECT_TRUE(name_set.count("table3") > 0);
}

} // namespace
} // namespace entropy
