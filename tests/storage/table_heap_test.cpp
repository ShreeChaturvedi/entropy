/**
 * @file table_heap_test.cpp
 * @brief Unit tests for TableHeap class
 */

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "catalog/schema.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/disk_manager.hpp"
#include "storage/table_heap.hpp"
#include "storage/tuple.hpp"

namespace entropy {
namespace {

class TableHeapTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temp file
        db_file_ = "/tmp/table_heap_test_" + std::to_string(rand());
        disk_manager_ = std::make_shared<DiskManager>(db_file_);
        buffer_pool_ = std::make_shared<BufferPoolManager>(10, disk_manager_);
        table_heap_ = std::make_unique<TableHeap>(buffer_pool_);

        // Create a simple schema for testing
        std::vector<Column> columns = {
            Column("id", TypeId::INTEGER),
            Column("name", TypeId::VARCHAR, 100),
            Column("age", TypeId::INTEGER)
        };
        schema_ = Schema(std::move(columns));
    }

    void TearDown() override {
        table_heap_.reset();
        buffer_pool_.reset();
        disk_manager_.reset();
        std::filesystem::remove(db_file_);
    }

    Tuple create_tuple(int32_t id, const std::string& name, int32_t age) {
        std::vector<TupleValue> values = {
            TupleValue(id),
            TupleValue(name),
            TupleValue(age)
        };
        return Tuple(values, schema_);
    }

    std::string db_file_;
    std::shared_ptr<DiskManager> disk_manager_;
    std::shared_ptr<BufferPoolManager> buffer_pool_;
    std::unique_ptr<TableHeap> table_heap_;
    Schema schema_;
};

TEST_F(TableHeapTest, InitialState) {
    EXPECT_TRUE(table_heap_->is_empty());
    EXPECT_EQ(table_heap_->first_page_id(), INVALID_PAGE_ID);
}

TEST_F(TableHeapTest, InsertSingleTuple) {
    Tuple tuple = create_tuple(1, "Alice", 25);
    RID rid;

    Status status = table_heap_->insert_tuple(tuple, &rid);

    EXPECT_TRUE(status.ok()) << status.message();
    EXPECT_TRUE(rid.is_valid());
    EXPECT_FALSE(table_heap_->is_empty());
}

TEST_F(TableHeapTest, InsertAndRetrieve) {
    Tuple original = create_tuple(42, "Bob", 30);
    RID rid;

    Status insert_status = table_heap_->insert_tuple(original, &rid);
    ASSERT_TRUE(insert_status.ok());

    Tuple retrieved;
    Status get_status = table_heap_->get_tuple(rid, &retrieved);
    ASSERT_TRUE(get_status.ok());

    // Compare the raw data
    EXPECT_EQ(original.size(), retrieved.size());
    EXPECT_EQ(std::memcmp(original.data(), retrieved.data(), original.size()), 0);
    EXPECT_EQ(retrieved.rid(), rid);
}

TEST_F(TableHeapTest, InsertMultipleTuples) {
    std::vector<RID> rids;

    for (int i = 0; i < 10; ++i) {
        Tuple tuple = create_tuple(i, "Name" + std::to_string(i), 20 + i);
        RID rid;
        Status status = table_heap_->insert_tuple(tuple, &rid);
        ASSERT_TRUE(status.ok()) << "Failed at i=" << i << ": " << status.message();
        rids.push_back(rid);
    }

    // Verify all tuples can be retrieved
    for (int i = 0; i < 10; ++i) {
        Tuple tuple;
        Status status = table_heap_->get_tuple(rids[i], &tuple);
        ASSERT_TRUE(status.ok()) << "Failed to get tuple at i=" << i;
        EXPECT_EQ(tuple.get_value(schema_, 0).as_integer(), i);
    }
}

TEST_F(TableHeapTest, DeleteTuple) {
    Tuple tuple = create_tuple(1, "ToDelete", 25);
    RID rid;

    ASSERT_TRUE(table_heap_->insert_tuple(tuple, &rid).ok());

    // Delete the tuple
    Status delete_status = table_heap_->delete_tuple(rid);
    EXPECT_TRUE(delete_status.ok());

    // Verify it's gone
    Tuple retrieved;
    Status get_status = table_heap_->get_tuple(rid, &retrieved);
    EXPECT_FALSE(get_status.ok());
    EXPECT_TRUE(get_status.is_not_found());
}

TEST_F(TableHeapTest, DeleteNonExistent) {
    RID invalid_rid(100, 50);  // Non-existent page/slot

    Status status = table_heap_->delete_tuple(invalid_rid);
    EXPECT_FALSE(status.ok());
}

TEST_F(TableHeapTest, UpdateTupleInPlace) {
    Tuple original = create_tuple(1, "Original", 25);
    RID rid;

    ASSERT_TRUE(table_heap_->insert_tuple(original, &rid).ok());

    // Update with similar-sized tuple
    Tuple updated = create_tuple(1, "UpdatedX", 26);
    Status update_status = table_heap_->update_tuple(updated, rid);
    EXPECT_TRUE(update_status.ok()) << update_status.message();

    // Verify update
    Tuple retrieved;
    ASSERT_TRUE(table_heap_->get_tuple(rid, &retrieved).ok());

    TupleValue age = retrieved.get_value(schema_, 2);
    EXPECT_EQ(age.as_integer(), 26);
}

TEST_F(TableHeapTest, UpdateTupleLarger) {
    Tuple original = create_tuple(1, "Short", 25);
    RID rid;

    ASSERT_TRUE(table_heap_->insert_tuple(original, &rid).ok());

    // Update with larger tuple (longer name)
    Tuple updated = create_tuple(1, "This is a much longer name that requires more space", 26);
    Status update_status = table_heap_->update_tuple(updated, rid);
    EXPECT_TRUE(update_status.ok()) << update_status.message();

    // Note: RID may have changed, but we should be able to find the tuple
    // by scanning (the update method doesn't return new RID currently)
}

TEST_F(TableHeapTest, UpdateNonExistent) {
    RID invalid_rid(100, 50);
    Tuple tuple = create_tuple(1, "Test", 25);

    Status status = table_heap_->update_tuple(tuple, invalid_rid);
    EXPECT_FALSE(status.ok());
}

TEST_F(TableHeapTest, GetTupleNonExistent) {
    RID invalid_rid(100, 50);
    Tuple tuple;

    Status status = table_heap_->get_tuple(invalid_rid, &tuple);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is_not_found());
}

TEST_F(TableHeapTest, GetTupleInvalidRID) {
    RID invalid_rid;  // Default constructor creates invalid RID
    Tuple tuple;

    Status status = table_heap_->get_tuple(invalid_rid, &tuple);
    EXPECT_FALSE(status.ok());
}

TEST_F(TableHeapTest, InsertEmptyTuple) {
    Tuple empty_tuple;
    RID rid;

    Status status = table_heap_->insert_tuple(empty_tuple, &rid);
    EXPECT_FALSE(status.ok());
}

TEST_F(TableHeapTest, InsertNullRIDPointer) {
    Tuple tuple = create_tuple(1, "Test", 25);

    Status status = table_heap_->insert_tuple(tuple, nullptr);
    EXPECT_FALSE(status.ok());
}

TEST_F(TableHeapTest, IteratorEmptyTable) {
    auto begin = table_heap_->begin();
    auto end = table_heap_->end();

    EXPECT_EQ(begin, end);
}

TEST_F(TableHeapTest, IteratorSingleTuple) {
    Tuple tuple = create_tuple(42, "Iterator", 30);
    RID rid;
    ASSERT_TRUE(table_heap_->insert_tuple(tuple, &rid).ok());

    int count = 0;
    for (auto it = table_heap_->begin(); it != table_heap_->end(); ++it) {
        EXPECT_EQ(it->get_value(schema_, 0).as_integer(), 42);
        count++;
    }
    EXPECT_EQ(count, 1);
}

TEST_F(TableHeapTest, IteratorMultipleTuples) {
    // Insert multiple tuples
    for (int i = 0; i < 5; ++i) {
        Tuple tuple = create_tuple(i, "Name" + std::to_string(i), 20 + i);
        RID rid;
        ASSERT_TRUE(table_heap_->insert_tuple(tuple, &rid).ok());
    }

    // Count tuples via iterator
    int count = 0;
    for (auto it = table_heap_->begin(); it != table_heap_->end(); ++it) {
        count++;
    }
    EXPECT_EQ(count, 5);
}

TEST_F(TableHeapTest, IteratorSkipsDeleted) {
    // Insert tuples
    std::vector<RID> rids;
    for (int i = 0; i < 5; ++i) {
        Tuple tuple = create_tuple(i, "Name" + std::to_string(i), 20 + i);
        RID rid;
        ASSERT_TRUE(table_heap_->insert_tuple(tuple, &rid).ok());
        rids.push_back(rid);
    }

    // Delete some tuples (indices 1 and 3)
    ASSERT_TRUE(table_heap_->delete_tuple(rids[1]).ok());
    ASSERT_TRUE(table_heap_->delete_tuple(rids[3]).ok());

    // Count remaining tuples
    int count = 0;
    std::vector<int32_t> ids;
    for (auto it = table_heap_->begin(); it != table_heap_->end(); ++it) {
        ids.push_back(it->get_value(schema_, 0).as_integer());
        count++;
    }

    EXPECT_EQ(count, 3);
    // Should have ids 0, 2, 4
    EXPECT_NE(std::find(ids.begin(), ids.end(), 0), ids.end());
    EXPECT_NE(std::find(ids.begin(), ids.end(), 2), ids.end());
    EXPECT_NE(std::find(ids.begin(), ids.end(), 4), ids.end());
    EXPECT_EQ(std::find(ids.begin(), ids.end(), 1), ids.end());
    EXPECT_EQ(std::find(ids.begin(), ids.end(), 3), ids.end());
}

TEST_F(TableHeapTest, IteratorPostIncrement) {
    for (int i = 0; i < 3; ++i) {
        Tuple tuple = create_tuple(i, "Name", 20);
        RID rid;
        ASSERT_TRUE(table_heap_->insert_tuple(tuple, &rid).ok());
    }

    auto it = table_heap_->begin();
    auto prev = it++;

    EXPECT_NE(prev.rid(), it.rid());
}

TEST_F(TableHeapTest, LargeTuples) {
    // Create tuple with longer string
    std::string long_name(500, 'X');
    Tuple tuple = create_tuple(1, long_name, 25);
    RID rid;

    Status status = table_heap_->insert_tuple(tuple, &rid);
    ASSERT_TRUE(status.ok()) << status.message();

    Tuple retrieved;
    ASSERT_TRUE(table_heap_->get_tuple(rid, &retrieved).ok());

    EXPECT_EQ(retrieved.get_value(schema_, 1).as_string(), long_name);
}

TEST_F(TableHeapTest, ManyTuplesSpanMultiplePages) {
    // Insert enough tuples to span multiple pages
    // With 4KB pages and ~20 byte tuples, we need ~200+ tuples
    constexpr int num_tuples = 300;
    std::vector<RID> rids;

    for (int i = 0; i < num_tuples; ++i) {
        Tuple tuple = create_tuple(i, "N" + std::to_string(i), i);
        RID rid;
        Status status = table_heap_->insert_tuple(tuple, &rid);
        ASSERT_TRUE(status.ok()) << "Failed at i=" << i << ": " << status.message();
        rids.push_back(rid);
    }

    // Verify all tuples via iterator
    int count = 0;
    for (auto it = table_heap_->begin(); it != table_heap_->end(); ++it) {
        count++;
    }
    EXPECT_EQ(count, num_tuples);

    // Verify random access
    Tuple t1, t2, t3;
    EXPECT_TRUE(table_heap_->get_tuple(rids[0], &t1).ok());
    EXPECT_TRUE(table_heap_->get_tuple(rids[150], &t2).ok());
    EXPECT_TRUE(table_heap_->get_tuple(rids[299], &t3).ok());

    EXPECT_EQ(t1.get_value(schema_, 0).as_integer(), 0);
    EXPECT_EQ(t2.get_value(schema_, 0).as_integer(), 150);
    EXPECT_EQ(t3.get_value(schema_, 0).as_integer(), 299);
}

TEST_F(TableHeapTest, DeleteAllTuples) {
    std::vector<RID> rids;

    for (int i = 0; i < 10; ++i) {
        Tuple tuple = create_tuple(i, "Name", 20);
        RID rid;
        ASSERT_TRUE(table_heap_->insert_tuple(tuple, &rid).ok());
        rids.push_back(rid);
    }

    // Delete all tuples
    for (const auto& rid : rids) {
        ASSERT_TRUE(table_heap_->delete_tuple(rid).ok());
    }

    // Table should be empty when iterating (though first_page_id still points to a page)
    int count = 0;
    for (auto it = table_heap_->begin(); it != table_heap_->end(); ++it) {
        count++;
    }
    EXPECT_EQ(count, 0);
}

TEST_F(TableHeapTest, ReinsertAfterDelete) {
    // Insert and delete
    Tuple tuple1 = create_tuple(1, "First", 25);
    RID rid1;
    ASSERT_TRUE(table_heap_->insert_tuple(tuple1, &rid1).ok());
    ASSERT_TRUE(table_heap_->delete_tuple(rid1).ok());

    // Insert again - should reuse the slot
    Tuple tuple2 = create_tuple(2, "Second", 30);
    RID rid2;
    ASSERT_TRUE(table_heap_->insert_tuple(tuple2, &rid2).ok());

    // Verify new tuple
    Tuple retrieved;
    ASSERT_TRUE(table_heap_->get_tuple(rid2, &retrieved).ok());
    EXPECT_EQ(retrieved.get_value(schema_, 0).as_integer(), 2);
}

TEST_F(TableHeapTest, IteratorRID) {
    Tuple tuple = create_tuple(1, "Test", 25);
    RID expected_rid;
    ASSERT_TRUE(table_heap_->insert_tuple(tuple, &expected_rid).ok());

    auto it = table_heap_->begin();
    EXPECT_TRUE(it.is_valid());
    EXPECT_EQ(it.rid(), expected_rid);
}

}  // namespace
}  // namespace entropy
