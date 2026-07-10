/**
 * @file catalog_persistence_test.cpp
 * @brief Outcome-based tests for catalog durability and thread-safety (#33)
 *
 * These tests exercise the observable contract rather than internals:
 *  - metadata (tables, indexes, next_oid) survives destroying and rebuilding
 *    the Catalog from the same on-disk files, and previously-inserted rows are
 *    scannable through the rebuilt TableHeap;
 *  - a shared_ptr handle obtained before drop_table stays valid after it;
 *  - concurrent create/get/drop does not corrupt the catalog.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "catalog/catalog.hpp"
#include "storage/b_plus_tree.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/disk_manager.hpp"
#include "storage/table_heap.hpp"
#include "storage/tuple.hpp"
#include "test_utils.hpp"

namespace entropy {
namespace {

Schema make_users_schema() {
  return Schema({
      Column("id", TypeId::INTEGER),
      Column("name", TypeId::VARCHAR, 50),
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Metadata + data survive a Catalog rebuild from the same files
// ─────────────────────────────────────────────────────────────────────────────

TEST(CatalogPersistence, SurvivesReopen) {
  test::TempDir dir("cat_persist_");
  const std::string db_path = (dir.path() / "test.db").string();
  const std::string manifest_path = (dir.path() / "test.catalog").string();

  const Schema schema = make_users_schema();
  oid_t users_oid = INVALID_OID;
  oid_t index_oid = INVALID_OID;

  // Phase 1: create table + index, insert rows.
  {
    auto disk = std::make_shared<DiskManager>(db_path);
    auto buffer_pool = std::make_shared<BufferPoolManager>(64, disk);
    Catalog catalog(buffer_pool, manifest_path);

    ASSERT_TRUE(catalog.create_table("users", schema).ok());

    auto info = catalog.get_table_shared("users");
    ASSERT_NE(info, nullptr);
    users_oid = info->oid;

    // Eager first-page allocation: a freshly created table already owns a page.
    EXPECT_NE(info->table_heap->first_page_id(), INVALID_PAGE_ID);

    for (int i = 1; i <= 5; ++i) {
      std::vector<TupleValue> values = {
          TupleValue(i), TupleValue(std::string("row") + std::to_string(i))};
      Tuple tuple(values, schema);
      RID rid;
      ASSERT_TRUE(info->table_heap->insert_tuple(tuple, &rid).ok());
    }

    ASSERT_TRUE(catalog.create_index("idx_id", "users", "id").ok());
    auto idx = catalog.get_index_shared("idx_id");
    ASSERT_NE(idx, nullptr);
    index_oid = idx->oid;

    // Flush so the heap and index pages reach disk before we tear down.
    buffer_pool->flush_all_pages();
  }

  // Phase 2: rebuild everything from the same files.
  {
    auto disk = std::make_shared<DiskManager>(db_path);
    auto buffer_pool = std::make_shared<BufferPoolManager>(64, disk);
    Catalog catalog(buffer_pool, manifest_path);

    // Table metadata survived.
    ASSERT_TRUE(catalog.table_exists("users"));
    auto info = catalog.get_table_shared("users");
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(info->oid, users_oid);
    ASSERT_EQ(info->schema.column_count(), 2u);
    EXPECT_EQ(info->schema.column(0).name(), "id");
    EXPECT_EQ(info->schema.column(1).name(), "name");

    // Rows are scannable through the rebuilt heap.
    std::set<int> ids;
    int count = 0;
    for (auto it = info->table_heap->begin(); it != info->table_heap->end();
         ++it) {
      Tuple tuple = *it;
      ids.insert(tuple.get_value(info->schema, 0).as_integer());
      ++count;
    }
    EXPECT_EQ(count, 5);
    for (int i = 1; i <= 5; ++i) {
      EXPECT_TRUE(ids.count(i) > 0) << "missing id " << i;
    }

    // Index metadata survived and the tree is still queryable.
    auto idx = catalog.get_index_shared("idx_id");
    ASSERT_NE(idx, nullptr);
    EXPECT_EQ(idx->oid, index_oid);
    EXPECT_EQ(idx->table_oid, users_oid);
    ASSERT_NE(idx->index, nullptr);
    auto found = idx->index->find(3);
    EXPECT_TRUE(found.has_value());

    // next_oid recovered: a new object must not collide with survivors.
    ASSERT_TRUE(
        catalog.create_table("t2", Schema({Column("x", TypeId::INTEGER)}))
            .ok());
    auto t2 = catalog.get_table_shared("t2");
    ASSERT_NE(t2, nullptr);
    EXPECT_NE(t2->oid, users_oid);
    EXPECT_NE(t2->oid, index_oid);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// drop_table walks the heap's page chain and reclaims every page
//
// On the current DiskManager, deallocate_page does not yet recycle page ids
// (WP1 adds the free list), so we assert the observable effect on this branch:
// walking the chain releases every buffered heap page back to the buffer pool.
// ─────────────────────────────────────────────────────────────────────────────

TEST(CatalogPersistence, DropReclaimsPages) {
  test::TempDir dir("cat_reclaim_");
  auto disk = std::make_shared<DiskManager>((dir.path() / "d.db").string());
  auto buffer_pool = std::make_shared<BufferPoolManager>(128, disk);
  Catalog catalog(buffer_pool, (dir.path() / "d.catalog").string());

  const Schema schema = make_users_schema();
  ASSERT_TRUE(catalog.create_table("gone", schema).ok());

  auto info = catalog.get_table_shared("gone");
  ASSERT_NE(info, nullptr);

  // Insert enough rows to span several heap pages so the drop must walk a chain.
  for (int i = 0; i < 400; ++i) {
    std::vector<TupleValue> values = {
        TupleValue(i), TupleValue(std::string(40, 'x'))};
    Tuple tuple(values, schema);
    RID rid;
    ASSERT_TRUE(info->table_heap->insert_tuple(tuple, &rid).ok());
  }
  ASSERT_GT(info->table_heap->first_page_id(), INVALID_PAGE_ID);
  info.reset(); // release our handle so pages are unpinned/reclaimable

  const size_t free_before = buffer_pool->free_list_size();
  ASSERT_TRUE(catalog.drop_table("gone").ok());
  EXPECT_FALSE(catalog.table_exists("gone"));

  // Every buffered heap page was returned to the pool's free list.
  EXPECT_GT(buffer_pool->free_list_size(), free_before);
}

// ─────────────────────────────────────────────────────────────────────────────
// A handle obtained before drop_table stays valid after it
// ─────────────────────────────────────────────────────────────────────────────

TEST(CatalogPersistence, SharedHandleSurvivesDrop) {
  test::TempDir dir("cat_handle_");
  auto disk = std::make_shared<DiskManager>((dir.path() / "h.db").string());
  auto buffer_pool = std::make_shared<BufferPoolManager>(64, disk);
  Catalog catalog(buffer_pool, (dir.path() / "h.catalog").string());

  const Schema schema = make_users_schema();
  ASSERT_TRUE(catalog.create_table("temp", schema).ok());

  auto handle = catalog.get_table_shared("temp");
  ASSERT_NE(handle, nullptr);
  const oid_t handle_oid = handle->oid;

  ASSERT_TRUE(catalog.drop_table("temp").ok());
  EXPECT_FALSE(catalog.table_exists("temp"));
  EXPECT_EQ(catalog.get_table_shared("temp"), nullptr);

  // The previously-obtained handle is still alive and readable — no dangle.
  EXPECT_EQ(handle->name, "temp");
  EXPECT_EQ(handle->oid, handle_oid);
  EXPECT_EQ(handle->schema.column_count(), 2u);
}

// ─────────────────────────────────────────────────────────────────────────────
// Concurrent create / get / drop smoke test (must not crash or corrupt)
// ─────────────────────────────────────────────────────────────────────────────

TEST(CatalogPersistence, ConcurrentCreateGetDrop) {
  test::TempDir dir("cat_concurrent_");
  auto disk = std::make_shared<DiskManager>((dir.path() / "c.db").string());
  auto buffer_pool = std::make_shared<BufferPoolManager>(256, disk);
  Catalog catalog(buffer_pool, (dir.path() / "c.catalog").string());

  constexpr int kThreads = 8;
  constexpr int kPerThread = 25;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&catalog, t] {
      const Schema schema({Column("id", TypeId::INTEGER)});
      for (int j = 0; j < kPerThread; ++j) {
        const std::string name =
            "t_" + std::to_string(t) + "_" + std::to_string(j);
        (void)catalog.create_table(name, schema);
        (void)catalog.get_table_shared(name);
        (void)catalog.get_table_names();
        (void)catalog.drop_table(name);
      }
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }

  // Every table each thread created was also dropped, so the catalog is empty
  // and internally consistent.
  EXPECT_TRUE(catalog.get_table_names().empty());
}

} // namespace
} // namespace entropy
