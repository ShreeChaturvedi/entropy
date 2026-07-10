/**
 * @file catalog.cpp
 * @brief Catalog implementation
 */

#include "catalog/catalog.hpp"

#include <algorithm>
#include <utility>

#include "catalog/catalog_manifest.hpp"
#include "storage/b_plus_tree.hpp"
#include "storage/table_heap.hpp"
#include "storage/tuple.hpp"

namespace entropy {

Catalog::Catalog(std::shared_ptr<BufferPoolManager> buffer_pool,
                 std::string manifest_path)
    : buffer_pool_(std::move(buffer_pool)),
      manifest_path_(std::move(manifest_path)) {
  load_from_manifest();
}

// ─────────────────────────────────────────────────────────────────────────────
// Durability
// ─────────────────────────────────────────────────────────────────────────────

void Catalog::load_from_manifest() {
  if (manifest_path_.empty()) {
    return;
  }

  CatalogManifest manifest;
  Status status = CatalogManifest::load(manifest_path_, &manifest);
  if (!status.ok()) {
    // No manifest yet (fresh database) — start with an empty catalog.
    return;
  }

  oid_t max_oid = 0;

  for (auto &t : manifest.tables) {
    auto heap = std::make_shared<TableHeap>(buffer_pool_, t.first_page_id);
    auto info = std::make_shared<TableInfo>(t.oid, t.name, t.schema,
                                            std::move(heap));
    table_names_[t.name] = t.oid;
    tables_[t.oid] = std::move(info);
    max_oid = std::max(max_oid, t.oid);
  }

  for (auto &idx : manifest.indexes) {
    auto tree = std::make_shared<BPlusTree>(buffer_pool_, idx.root_page_id);
    auto info = std::make_shared<IndexInfo>(idx.oid, idx.name, idx.table_oid,
                                            idx.key_column, std::move(tree));
    index_names_[idx.name] = idx.oid;
    indexes_[idx.oid] = std::move(info);
    max_oid = std::max(max_oid, idx.oid);
  }

  // Recover next_oid, guarded by the max observed oid so ids never alias.
  next_oid_ = std::max(manifest.next_oid, max_oid + 1);
}

CatalogManifest Catalog::snapshot() const {
  CatalogManifest manifest;
  manifest.next_oid = next_oid_;

  for (const auto &[oid, info] : tables_) {
    ManifestTable t;
    t.oid = info->oid;
    t.name = info->name;
    t.schema = info->schema;
    t.first_page_id =
        info->table_heap ? info->table_heap->first_page_id() : INVALID_PAGE_ID;
    manifest.tables.push_back(std::move(t));
  }

  for (const auto &[oid, info] : indexes_) {
    ManifestIndex idx;
    idx.oid = info->oid;
    idx.name = info->name;
    idx.table_oid = info->table_oid;
    idx.key_column = info->key_column;
    idx.root_page_id =
        info->index ? info->index->root_page_id() : INVALID_PAGE_ID;
    manifest.indexes.push_back(std::move(idx));
  }

  return manifest;
}

Status Catalog::persist() const {
  if (manifest_path_.empty()) {
    return Status::Ok();
  }
  return snapshot().save(manifest_path_);
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal lookups (caller holds mutex_)
// ─────────────────────────────────────────────────────────────────────────────

std::shared_ptr<TableInfo>
Catalog::find_table(const std::string &table_name) const {
  auto it = table_names_.find(table_name);
  if (it == table_names_.end()) {
    return nullptr;
  }
  auto table_it = tables_.find(it->second);
  return (table_it != tables_.end()) ? table_it->second : nullptr;
}

std::shared_ptr<TableInfo> Catalog::find_table(oid_t table_oid) const {
  auto it = tables_.find(table_oid);
  return (it != tables_.end()) ? it->second : nullptr;
}

std::shared_ptr<IndexInfo>
Catalog::find_index(const std::string &index_name) const {
  auto it = index_names_.find(index_name);
  if (it == index_names_.end()) {
    return nullptr;
  }
  auto idx_it = indexes_.find(it->second);
  return (idx_it != indexes_.end()) ? idx_it->second : nullptr;
}

std::shared_ptr<IndexInfo> Catalog::find_index(oid_t index_oid) const {
  auto it = indexes_.find(index_oid);
  return (it != indexes_.end()) ? it->second : nullptr;
}

// ─────────────────────────────────────────────────────────────────────────────
// Table Management
// ─────────────────────────────────────────────────────────────────────────────

Status Catalog::create_table(const std::string &table_name,
                             const Schema &schema) {
  std::unique_lock lock(mutex_);

  if (find_table(table_name) != nullptr) {
    return Status::AlreadyExists("Table already exists: " + table_name);
  }

  // Allocate a new OID.
  oid_t oid = next_oid_++;

  // Create a TableHeap and eagerly allocate its first page so first_page_id
  // is deterministic and recordable in the manifest.
  auto table_heap = std::make_shared<TableHeap>(buffer_pool_);
  Status alloc = table_heap->ensure_first_page();
  if (!alloc.ok()) {
    next_oid_--;
    return alloc;
  }

  auto info = std::make_shared<TableInfo>(oid, table_name, schema, table_heap);

  table_names_[table_name] = oid;
  tables_[oid] = info;

  Status persisted = persist();
  if (!persisted.ok()) {
    // Roll back so in-memory state stays consistent with what is on disk.
    tables_.erase(oid);
    table_names_.erase(table_name);
    (void)table_heap->reclaim_all_pages();
    next_oid_--;
    return persisted;
  }

  return Status::Ok();
}

Status Catalog::drop_table(const std::string &table_name) {
  std::unique_lock lock(mutex_);

  auto name_it = table_names_.find(table_name);
  if (name_it == table_names_.end()) {
    return Status::NotFound("Table not found: " + table_name);
  }

  oid_t oid = name_it->second;
  // Keep the TableInfo alive locally so we can reclaim its pages after the
  // map entries are removed.
  std::shared_ptr<TableInfo> info = find_table(oid);

  tables_.erase(oid);
  table_names_.erase(name_it);

  // Durably record the drop before reclaiming pages. If we crash after this
  // point but before reclamation, the pages simply leak (safe); the table is
  // gone. If persist fails, restore the entries so memory matches disk.
  Status persisted = persist();
  if (!persisted.ok()) {
    if (info) {
      table_names_[table_name] = oid;
      tables_[oid] = std::move(info);
    }
    return persisted;
  }

  // Reclaim the heap's pages through the buffer pool (DiskManager::
  // deallocate_page) so they are not orphaned on disk.
  if (info && info->table_heap) {
    (void)info->table_heap->reclaim_all_pages();
  }

  return Status::Ok();
}

std::shared_ptr<TableInfo>
Catalog::get_table_shared(const std::string &table_name) const {
  std::shared_lock lock(mutex_);
  return find_table(table_name);
}

std::shared_ptr<TableInfo> Catalog::get_table_shared(oid_t table_oid) const {
  std::shared_lock lock(mutex_);
  return find_table(table_oid);
}

TableInfo *Catalog::get_table(const std::string &table_name) {
  return get_table_shared(table_name).get();
}

const TableInfo *Catalog::get_table(const std::string &table_name) const {
  return get_table_shared(table_name).get();
}

TableInfo *Catalog::get_table(oid_t table_oid) {
  return get_table_shared(table_oid).get();
}

const TableInfo *Catalog::get_table(oid_t table_oid) const {
  return get_table_shared(table_oid).get();
}

const Schema *Catalog::get_table_schema(const std::string &table_name) const {
  std::shared_lock lock(mutex_);
  auto info = find_table(table_name);
  return info ? &info->schema : nullptr;
}

bool Catalog::table_exists(const std::string &table_name) const {
  std::shared_lock lock(mutex_);
  return table_names_.find(table_name) != table_names_.end();
}

oid_t Catalog::get_table_oid(const std::string &table_name) const {
  std::shared_lock lock(mutex_);
  auto it = table_names_.find(table_name);
  return it != table_names_.end() ? it->second : INVALID_OID;
}

std::vector<std::string> Catalog::get_table_names() const {
  std::shared_lock lock(mutex_);
  std::vector<std::string> names;
  names.reserve(table_names_.size());
  for (const auto &[name, oid] : table_names_) {
    names.push_back(name);
  }
  return names;
}

// ─────────────────────────────────────────────────────────────────────────────
// Index Management
// ─────────────────────────────────────────────────────────────────────────────

Status Catalog::create_index(const std::string &index_name,
                             const std::string &table_name,
                             const std::string &column_name) {
  std::unique_lock lock(mutex_);

  // Check if index already exists.
  if (index_names_.find(index_name) != index_names_.end()) {
    return Status::AlreadyExists("Index already exists: " + index_name);
  }

  // Get table.
  auto table_info = find_table(table_name);
  if (!table_info) {
    return Status::NotFound("Table not found: " + table_name);
  }

  // Find column.
  int col_idx = table_info->schema.get_column_index(column_name);
  if (col_idx < 0) {
    return Status::NotFound("Column not found: " + column_name);
  }

  // Create B+ tree index.
  auto index = std::make_shared<BPlusTree>(buffer_pool_);

  // Allocate new OID.
  oid_t oid = next_oid_++;

  // Create IndexInfo.
  auto info = std::make_shared<IndexInfo>(
      oid, index_name, table_info->oid, static_cast<column_id_t>(col_idx),
      index);

  // Build index from existing data.
  for (auto it = table_info->table_heap->begin();
       it != table_info->table_heap->end(); ++it) {
    Tuple tuple = *it;
    TupleValue key_val =
        tuple.get_value(table_info->schema, static_cast<uint32_t>(col_idx));
    // Convert to BPTreeKey (int64_t).
    BPTreeKey key = 0;
    if (key_val.is_integer()) {
      key = static_cast<BPTreeKey>(key_val.as_integer());
    } else if (key_val.is_bigint()) {
      key = key_val.as_bigint();
    }
    (void)index->insert(key, it.rid());
  }

  // Store in catalog.
  index_names_[index_name] = oid;
  indexes_[oid] = info;

  Status persisted = persist();
  if (!persisted.ok()) {
    indexes_.erase(oid);
    index_names_.erase(index_name);
    next_oid_--;
    return persisted;
  }

  return Status::Ok();
}

std::shared_ptr<IndexInfo>
Catalog::get_index_shared(const std::string &index_name) const {
  std::shared_lock lock(mutex_);
  return find_index(index_name);
}

std::shared_ptr<IndexInfo> Catalog::get_index_shared(oid_t index_oid) const {
  std::shared_lock lock(mutex_);
  return find_index(index_oid);
}

IndexInfo *Catalog::get_index(const std::string &index_name) {
  return get_index_shared(index_name).get();
}

const IndexInfo *Catalog::get_index(const std::string &index_name) const {
  return get_index_shared(index_name).get();
}

IndexInfo *Catalog::get_index_by_oid(oid_t index_oid) {
  return get_index_shared(index_oid).get();
}

const IndexInfo *Catalog::get_index_by_oid(oid_t index_oid) const {
  return get_index_shared(index_oid).get();
}

IndexInfo *Catalog::get_index_for_column(oid_t table_oid,
                                         column_id_t column_id) {
  std::shared_lock lock(mutex_);
  for (auto &[oid, info] : indexes_) {
    if (info->table_oid == table_oid && info->key_column == column_id) {
      return info.get();
    }
  }
  return nullptr;
}

const IndexInfo *Catalog::get_index_for_column(oid_t table_oid,
                                               column_id_t column_id) const {
  std::shared_lock lock(mutex_);
  for (const auto &[oid, info] : indexes_) {
    if (info->table_oid == table_oid && info->key_column == column_id) {
      return info.get();
    }
  }
  return nullptr;
}

std::vector<IndexInfo *> Catalog::get_table_indexes(oid_t table_oid) {
  std::shared_lock lock(mutex_);
  std::vector<IndexInfo *> result;
  for (auto &[oid, info] : indexes_) {
    if (info->table_oid == table_oid) {
      result.push_back(info.get());
    }
  }
  return result;
}

} // namespace entropy
