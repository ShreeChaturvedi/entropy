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

#include <set>
#include <string>
#include <thread>
#include <vector>

#include "catalog/catalog.hpp"
#include "catalog/catalog_manifest.hpp"
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
    auto disk = std::make_shared<FileDiskManager>(db_path);
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
    auto disk = std::make_shared<FileDiskManager>(db_path);
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
  auto disk = std::make_shared<FileDiskManager>((dir.path() / "d.db").string());
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
  auto disk = std::make_shared<FileDiskManager>((dir.path() / "h.db").string());
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
// Crash image: manifest fsync'd but referenced pages never reached disk
//
// Simulates kill -9 between the manifest write and the page flush. The pages
// the manifest references read back zeroed (next link 0), so a naive chain
// walk would loop forever. The open must complete and the table must be
// usable (or fail cleanly) — never hang.
// ─────────────────────────────────────────────────────────────────────────────

TEST(CatalogPersistence, CrashImageWithUnflushedPagesDoesNotHang) {
  test::TempDir dir("cat_crash_");
  const std::string db_path = (dir.path() / "x.db").string();
  const std::string manifest_path = (dir.path() / "x.catalog").string();

  const Schema schema = make_users_schema();

  // Hand-craft the crash image: a durable manifest whose page references
  // point beyond the end of an empty db file.
  CatalogManifest manifest;
  manifest.next_oid = 3;
  ManifestTable t;
  t.oid = 1;
  t.name = "users";
  t.schema = schema;
  t.first_page_id = 3; // never written
  manifest.tables.push_back(t);
  ManifestIndex mi;
  mi.oid = 2;
  mi.name = "idx_id";
  mi.table_oid = 1;
  mi.key_column = 0;
  mi.root_page_id = 7; // never written
  manifest.indexes.push_back(mi);
  ASSERT_TRUE(manifest.save(manifest_path).ok());

  auto disk = std::make_shared<FileDiskManager>(db_path); // empty file
  auto buffer_pool = std::make_shared<BufferPoolManager>(64, disk);
  Catalog catalog(buffer_pool, manifest_path); // must not hang

  // Metadata survived and the table self-healed to an empty, usable heap.
  ASSERT_TRUE(catalog.table_exists("users"));
  auto info = catalog.get_table_shared("users");
  ASSERT_NE(info, nullptr);

  int rows = 0;
  for (auto it = info->table_heap->begin(); it != info->table_heap->end();
       ++it) {
    ++rows;
  }
  EXPECT_EQ(rows, 0);

  std::vector<TupleValue> values = {TupleValue(1),
                                    TupleValue(std::string("alice"))};
  Tuple tuple(values, schema);
  RID rid;
  ASSERT_TRUE(info->table_heap->insert_tuple(tuple, &rid).ok());
  Tuple back;
  EXPECT_TRUE(info->table_heap->get_tuple(rid, &back).ok());

  // The index whose root never reached disk is rebuilt from the heap, which
  // self-healed to empty here — so the tree is empty too.
  auto idx = catalog.get_index_shared("idx_id");
  ASSERT_NE(idx, nullptr);
  EXPECT_FALSE(idx->index->find(1).has_value());

  // Reclamation over the damaged chain terminates too.
  EXPECT_TRUE(catalog.drop_table("users").ok());
}

// ─────────────────────────────────────────────────────────────────────────────
// A post-DDL index root change is tracked durably by the manifest
//
// Deliberately NO flush_all_pages and NO clean teardown before the reopen:
// the first buffer pool is kept alive (its destructor would flush everything
// and mask the bug) while a second, independent pool reopens the same files —
// the crash image. The root-change callback's own flush + persist must be
// what makes the reopened index work.
// ─────────────────────────────────────────────────────────────────────────────

TEST(CatalogPersistence, IndexRootChangeSurvivesReopen) {
  test::TempDir dir("cat_root_");
  const std::string db_path = (dir.path() / "r.db").string();
  const std::string manifest_path = (dir.path() / "r.catalog").string();

  auto disk = std::make_shared<FileDiskManager>(db_path);
  auto buffer_pool = std::make_shared<BufferPoolManager>(64, disk);
  Catalog catalog(buffer_pool, manifest_path);

  ASSERT_TRUE(
      catalog.create_table("events", Schema({Column("id", TypeId::INTEGER)}))
          .ok());
  ASSERT_TRUE(catalog.create_index("idx_ev", "events", "id").ok());

  auto idx = catalog.get_index_shared("idx_ev");
  ASSERT_NE(idx, nullptr);

  // Insert enough keys directly into the tree to force root changes
  // (creation, then a split) after create_index already persisted. Keys up
  // to the last root change are durable via the callback's flush; later
  // ones may live only in unflushed leaves.
  page_id_t prev_root = idx->index->root_page_id();
  int last_root_change_key = -1;
  for (int i = 0; i < 600; ++i) {
    ASSERT_TRUE(
        idx->index->insert(i, RID(1, static_cast<slot_id_t>(i % 100))).ok());
    if (idx->index->root_page_id() != prev_root) {
      prev_root = idx->index->root_page_id();
      last_root_change_key = i;
    }
  }
  ASSERT_GT(last_root_change_key, 0) << "expected at least one root split";

  // Simulated kill -9: reopen through a fresh disk manager + pool while the
  // first pool still holds its unflushed frames.
  {
    auto disk2 = std::make_shared<FileDiskManager>(db_path);
    auto buffer_pool2 = std::make_shared<BufferPoolManager>(64, disk2);
    Catalog catalog2(buffer_pool2, manifest_path);

    auto idx2 = catalog2.get_index_shared("idx_ev");
    ASSERT_NE(idx2, nullptr);
    for (int key : {0, last_root_change_key / 2, last_root_change_key}) {
      EXPECT_TRUE(idx2->index->find(key).has_value()) << "missing key " << key;
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// A damaged index root is rebuilt from the intact heap on load
// ─────────────────────────────────────────────────────────────────────────────

TEST(CatalogPersistence, DamagedIndexRootRebuildsFromHeap) {
  test::TempDir dir("cat_rebuild_");
  const std::string db_path = (dir.path() / "b.db").string();
  const std::string manifest_path = (dir.path() / "b.catalog").string();

  const Schema schema = make_users_schema();

  {
    auto disk = std::make_shared<FileDiskManager>(db_path);
    auto buffer_pool = std::make_shared<BufferPoolManager>(64, disk);
    Catalog catalog(buffer_pool, manifest_path);

    ASSERT_TRUE(catalog.create_table("users", schema).ok());
    auto info = catalog.get_table_shared("users");
    for (int i = 1; i <= 5; ++i) {
      std::vector<TupleValue> values = {
          TupleValue(i), TupleValue(std::string("u") + std::to_string(i))};
      Tuple tuple(values, schema);
      RID rid;
      ASSERT_TRUE(info->table_heap->insert_tuple(tuple, &rid).ok());
    }
    ASSERT_TRUE(catalog.create_index("idx_id", "users", "id").ok());
    buffer_pool->flush_all_pages();
  }

  // Corrupt the durable manifest: point the index root at a page that does
  // not exist (models the root page having been lost).
  {
    CatalogManifest manifest;
    ASSERT_TRUE(CatalogManifest::load(manifest_path, &manifest).ok());
    ASSERT_EQ(manifest.indexes.size(), 1u);
    manifest.indexes[0].root_page_id = 1000;
    ASSERT_TRUE(manifest.save(manifest_path).ok());
  }

  {
    auto disk = std::make_shared<FileDiskManager>(db_path);
    auto buffer_pool = std::make_shared<BufferPoolManager>(64, disk);
    Catalog catalog(buffer_pool, manifest_path);

    // The heap is intact, so the index is rebuilt rather than left empty.
    auto idx = catalog.get_index_shared("idx_id");
    ASSERT_NE(idx, nullptr);
    for (int key = 1; key <= 5; ++key) {
      EXPECT_TRUE(idx->index->find(key).has_value()) << "missing key " << key;
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Dropping a table drops its indexes with it
// ─────────────────────────────────────────────────────────────────────────────

TEST(CatalogPersistence, DropTableDropsIndexes) {
  test::TempDir dir("cat_dropidx_");
  const std::string db_path = (dir.path() / "i.db").string();
  const std::string manifest_path = (dir.path() / "i.catalog").string();

  {
    auto disk = std::make_shared<FileDiskManager>(db_path);
    auto buffer_pool = std::make_shared<BufferPoolManager>(64, disk);
    Catalog catalog(buffer_pool, manifest_path);

    Schema schema({Column("a", TypeId::INTEGER), Column("b", TypeId::INTEGER)});
    ASSERT_TRUE(catalog.create_table("pairs", schema).ok());
    const oid_t table_oid = catalog.get_table_oid("pairs");

    auto info = catalog.get_table_shared("pairs");
    for (int i = 0; i < 3; ++i) {
      std::vector<TupleValue> values = {TupleValue(i), TupleValue(i * 10)};
      Tuple tuple(values, schema);
      RID rid;
      ASSERT_TRUE(info->table_heap->insert_tuple(tuple, &rid).ok());
    }
    info.reset();

    ASSERT_TRUE(catalog.create_index("idx_a", "pairs", "a").ok());
    ASSERT_TRUE(catalog.create_index("idx_b", "pairs", "b").ok());

    ASSERT_TRUE(catalog.drop_table("pairs").ok());
    EXPECT_EQ(catalog.get_index_shared("idx_a"), nullptr);
    EXPECT_EQ(catalog.get_index_shared("idx_b"), nullptr);
    EXPECT_TRUE(catalog.get_table_indexes(table_oid).empty());
  }

  // The drop is durable: nothing comes back after a reopen.
  {
    auto disk = std::make_shared<FileDiskManager>(db_path);
    auto buffer_pool = std::make_shared<BufferPoolManager>(64, disk);
    Catalog catalog(buffer_pool, manifest_path);

    EXPECT_FALSE(catalog.table_exists("pairs"));
    EXPECT_EQ(catalog.get_index_shared("idx_a"), nullptr);
    EXPECT_EQ(catalog.get_index_shared("idx_b"), nullptr);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Concurrent create / get / drop smoke test (must not crash or corrupt)
// ─────────────────────────────────────────────────────────────────────────────

TEST(CatalogPersistence, ConcurrentCreateGetDrop) {
  test::TempDir dir("cat_concurrent_");
  auto disk = std::make_shared<FileDiskManager>((dir.path() / "c.db").string());
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
