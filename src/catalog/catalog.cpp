/**
 * @file catalog.cpp
 * @brief Catalog implementation
 */

#include "catalog/catalog.hpp"

#include "storage/b_plus_tree.hpp"
#include "storage/table_heap.hpp"
#include "storage/tuple.hpp"

namespace entropy {

Catalog::Catalog(std::shared_ptr<BufferPoolManager> buffer_pool)
    : buffer_pool_(std::move(buffer_pool)) {}

Status Catalog::create_table(const std::string &table_name,
                             const Schema &schema) {
  if (table_exists(table_name)) {
    return Status::AlreadyExists("Table already exists: " + table_name);
  }

  // Allocate a new OID
  oid_t oid = next_oid_++;

  // Create a TableHeap for storing the table's data
  auto table_heap = std::make_shared<TableHeap>(buffer_pool_);

  // Create the TableInfo
  TableInfo info(oid, table_name, schema, table_heap);

  // Store in catalog
  table_names_[table_name] = oid;
  tables_[oid] = std::move(info);

  return Status::Ok();
}

Status Catalog::drop_table(const std::string &table_name) {
  auto it = table_names_.find(table_name);
  if (it == table_names_.end()) {
    return Status::NotFound("Table not found: " + table_name);
  }

  oid_t oid = it->second;

  // Remove from both maps
  tables_.erase(oid);
  table_names_.erase(it);

  // Note: The TableHeap's pages are not deallocated here.
  // A full implementation would need to mark those pages as free.

  return Status::Ok();
}

TableInfo *Catalog::get_table(const std::string &table_name) {
  auto it = table_names_.find(table_name);
  if (it == table_names_.end()) {
    return nullptr;
  }

  auto table_it = tables_.find(it->second);
  if (table_it == tables_.end()) {
    return nullptr;
  }

  return &table_it->second;
}

const TableInfo *Catalog::get_table(const std::string &table_name) const {
  auto it = table_names_.find(table_name);
  if (it == table_names_.end()) {
    return nullptr;
  }

  auto table_it = tables_.find(it->second);
  if (table_it == tables_.end()) {
    return nullptr;
  }

  return &table_it->second;
}

const Schema *Catalog::get_table_schema(const std::string &table_name) const {
  const TableInfo *info = get_table(table_name);
  return info ? &info->schema : nullptr;
}

bool Catalog::table_exists(const std::string &table_name) const {
  return table_names_.find(table_name) != table_names_.end();
}

oid_t Catalog::get_table_oid(const std::string &table_name) const {
  auto it = table_names_.find(table_name);
  return it != table_names_.end() ? it->second : INVALID_OID;
}

std::vector<std::string> Catalog::get_table_names() const {
  std::vector<std::string> names;
  names.reserve(table_names_.size());
  for (const auto &[name, oid] : table_names_) {
    names.push_back(name);
  }
  return names;
}

// ─────────────────────────────────────────────────────────────────────────────
// Table lookup by OID
// ─────────────────────────────────────────────────────────────────────────────

TableInfo *Catalog::get_table(oid_t table_oid) {
  auto it = tables_.find(table_oid);
  return (it != tables_.end()) ? &it->second : nullptr;
}

const TableInfo *Catalog::get_table(oid_t table_oid) const {
  auto it = tables_.find(table_oid);
  return (it != tables_.end()) ? &it->second : nullptr;
}

// ─────────────────────────────────────────────────────────────────────────────
// Index Management
// ─────────────────────────────────────────────────────────────────────────────

Status Catalog::create_index(const std::string &index_name,
                             const std::string &table_name,
                             const std::string &column_name) {
  // Check if index already exists
  if (index_names_.find(index_name) != index_names_.end()) {
    return Status::AlreadyExists("Index already exists: " + index_name);
  }

  // Get table
  auto *table_info = get_table(table_name);
  if (!table_info) {
    return Status::NotFound("Table not found: " + table_name);
  }

  // Find column
  int col_idx = table_info->schema.get_column_index(column_name);
  if (col_idx < 0) {
    return Status::NotFound("Column not found: " + column_name);
  }

  // Create B+ tree index
  auto index = std::make_shared<BPlusTree>(buffer_pool_);

  // Allocate new OID
  oid_t oid = next_oid_++;

  // Create IndexInfo
  IndexInfo info(oid, index_name, table_info->oid,
                 static_cast<column_id_t>(col_idx), index);

  // Build index from existing data
  for (auto it = table_info->table_heap->begin();
       it != table_info->table_heap->end(); ++it) {
    Tuple tuple = *it;
    TupleValue key_val =
        tuple.get_value(table_info->schema, static_cast<uint32_t>(col_idx));
    // Convert to BPTreeKey (int64_t)
    BPTreeKey key = 0;
    if (key_val.is_integer()) {
      key = static_cast<BPTreeKey>(key_val.as_integer());
    } else if (key_val.is_bigint()) {
      key = static_cast<BPTreeKey>(key_val.as_bigint());
    }
    (void)index->insert(key, it.rid());
  }

  // Store in catalog
  index_names_[index_name] = oid;
  indexes_[oid] = std::move(info);

  return Status::Ok();
}

IndexInfo *Catalog::get_index(const std::string &index_name) {
  auto it = index_names_.find(index_name);
  if (it == index_names_.end())
    return nullptr;
  auto idx_it = indexes_.find(it->second);
  return (idx_it != indexes_.end()) ? &idx_it->second : nullptr;
}

const IndexInfo *Catalog::get_index(const std::string &index_name) const {
  auto it = index_names_.find(index_name);
  if (it == index_names_.end())
    return nullptr;
  auto idx_it = indexes_.find(it->second);
  return (idx_it != indexes_.end()) ? &idx_it->second : nullptr;
}

IndexInfo *Catalog::get_index_for_column(oid_t table_oid,
                                         column_id_t column_id) {
  for (auto &[oid, info] : indexes_) {
    if (info.table_oid == table_oid && info.key_column == column_id) {
      return &info;
    }
  }
  return nullptr;
}

const IndexInfo *Catalog::get_index_for_column(oid_t table_oid,
                                               column_id_t column_id) const {
  for (const auto &[oid, info] : indexes_) {
    if (info.table_oid == table_oid && info.key_column == column_id) {
      return &info;
    }
  }
  return nullptr;
}

std::vector<IndexInfo *> Catalog::get_table_indexes(oid_t table_oid) {
  std::vector<IndexInfo *> result;
  for (auto &[oid, info] : indexes_) {
    if (info.table_oid == table_oid) {
      result.push_back(&info);
    }
  }
  return result;
}

} // namespace entropy
