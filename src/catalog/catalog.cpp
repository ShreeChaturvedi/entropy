/**
 * @file catalog.cpp
 * @brief Catalog implementation
 */

#include "catalog/catalog.hpp"

#include "storage/table_heap.hpp"

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

} // namespace entropy
