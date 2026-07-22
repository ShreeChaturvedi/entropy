#pragma once

/**
 * @file catalog.hpp
 * @brief System catalog for metadata management
 *
 * The catalog stores metadata about tables and indexes:
 * - TableInfo: name, schema, and TableHeap for data storage
 * - IndexInfo: name, key column, and B+ tree index
 */

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/schema.hpp"
#include "common/types.hpp"
#include "entropy/status.hpp"

namespace entropy {

// Forward declarations
class BufferPoolManager;
class TableHeap;
class BPlusTree;
struct CatalogManifest;

// ─────────────────────────────────────────────────────────────────────────────
// TableInfo - Complete table metadata
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Complete information about a table
 */
struct TableInfo {
  oid_t oid;                             ///< Unique table identifier
  std::string name;                      ///< Table name
  Schema schema;                         ///< Table schema (column definitions)
  std::shared_ptr<TableHeap> table_heap; ///< Storage for table data

  TableInfo() = default;
  TableInfo(oid_t id, std::string n, Schema s, std::shared_ptr<TableHeap> heap)
      : oid(id), name(std::move(n)), schema(std::move(s)),
        table_heap(std::move(heap)) {}
};

// ─────────────────────────────────────────────────────────────────────────────
// IndexInfo - Index metadata
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Complete information about an index
 */
struct IndexInfo {
  oid_t oid;                        ///< Unique index identifier
  std::string name;                 ///< Index name
  oid_t table_oid;                  ///< Table this index belongs to
  column_id_t key_column;           ///< Column being indexed
  std::shared_ptr<BPlusTree> index; ///< B+ tree index structure

  IndexInfo() = default;
  IndexInfo(oid_t id, std::string n, oid_t tbl, column_id_t col,
            std::shared_ptr<BPlusTree> idx)
      : oid(id), name(std::move(n)), table_oid(tbl), key_column(col),
        index(std::move(idx)) {}
};

// ─────────────────────────────────────────────────────────────────────────────
// Catalog - System catalog manager
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief System catalog - manages table and index metadata
 */
class Catalog {
public:
  /**
   * @brief Construct a catalog
   * @param buffer_pool Buffer pool used to back table heaps and indexes
   * @param manifest_path Path to the durable catalog manifest. When non-empty,
   *        the catalog loads it (if present) on construction and writes +
   *        fsyncs it on every DDL. When empty, the catalog is in-memory only.
   */
  explicit Catalog(std::shared_ptr<BufferPoolManager> buffer_pool,
                   std::string manifest_path = "");
  ~Catalog();

  // ─────────────────────────────────────────────────────────────────────────
  // Table Management
  // ─────────────────────────────────────────────────────────────────────────

  [[nodiscard]] Status create_table(const std::string &table_name,
                                    const Schema &schema);
  [[nodiscard]] Status drop_table(const std::string &table_name);

  /**
   * @brief Look up a table, returning a stable owning handle
   *
   * The returned shared_ptr keeps the TableInfo alive even if another thread
   * drops the table concurrently, so callers never dereference freed memory.
   */
  [[nodiscard]] std::shared_ptr<TableInfo>
  get_table_shared(const std::string &table_name) const;
  [[nodiscard]] std::shared_ptr<TableInfo> get_table_shared(oid_t table_oid)
      const;

  // Transitional raw-pointer getters: thin wrappers over the shared_ptr
  // getters. A returned pointer dangles once the table is dropped; the Binder
  // and executors move to get_table_shared in a later work package, after
  // which these are removed.
  [[nodiscard]] TableInfo *get_table(const std::string &table_name);
  [[nodiscard]] const TableInfo *get_table(const std::string &table_name) const;
  [[nodiscard]] TableInfo *get_table(oid_t table_oid);
  [[nodiscard]] const TableInfo *get_table(oid_t table_oid) const;
  [[nodiscard]] const Schema *
  get_table_schema(const std::string &table_name) const;
  [[nodiscard]] bool table_exists(const std::string &table_name) const;
  [[nodiscard]] oid_t get_table_oid(const std::string &table_name) const;
  [[nodiscard]] std::vector<std::string> get_table_names() const;

  // ─────────────────────────────────────────────────────────────────────────
  // Index Management
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Create an index on a column
   * @param index_name Name of the index
   * @param table_name Table to index
   * @param column_name Column to index
   * @return Status::Ok() on success
   */
  [[nodiscard]] Status create_index(const std::string &index_name,
                                    const std::string &table_name,
                                    const std::string &column_name);

  /**
   * @brief Look up an index, returning a stable owning handle
   */
  [[nodiscard]] std::shared_ptr<IndexInfo>
  get_index_shared(const std::string &index_name) const;
  [[nodiscard]] std::shared_ptr<IndexInfo> get_index_shared(oid_t index_oid)
      const;

  /**
   * @brief Get index info by name
   *
   * Transitional raw getter; see get_table for the migration note.
   */
  [[nodiscard]] IndexInfo *get_index(const std::string &index_name);
  [[nodiscard]] const IndexInfo *get_index(const std::string &index_name) const;

  /**
   * @brief Get index info by OID
   *
   * Index OIDs and column ids are distinct namespaces — never cast an
   * index OID to column_id_t and pass it to get_index_for_column.
   */
  [[nodiscard]] IndexInfo *get_index_by_oid(oid_t index_oid);
  [[nodiscard]] const IndexInfo *get_index_by_oid(oid_t index_oid) const;

  /**
   * @brief Get index for a specific column
   * @param column_id Column index within the table schema (not an index OID)
   * @return IndexInfo if column has an index, nullptr otherwise
   */
  [[nodiscard]] IndexInfo *get_index_for_column(oid_t table_oid,
                                                column_id_t column_id);
  [[nodiscard]] const IndexInfo *
  get_index_for_column(oid_t table_oid, column_id_t column_id) const;

  /**
   * @brief Get all indexes for a table
   */
  [[nodiscard]] std::vector<IndexInfo *> get_table_indexes(oid_t table_oid);

  // ─────────────────────────────────────────────────────────────────────────
  // Accessors
  // ─────────────────────────────────────────────────────────────────────────

  [[nodiscard]] std::shared_ptr<BufferPoolManager> buffer_pool() const {
    return buffer_pool_;
  }

private:
  // ─────────────────────────────────────────────────────────────────────────
  // Internal helpers (callers must hold mutex_)
  // ─────────────────────────────────────────────────────────────────────────

  [[nodiscard]] std::shared_ptr<TableInfo>
  find_table(const std::string &table_name) const;
  [[nodiscard]] std::shared_ptr<TableInfo> find_table(oid_t table_oid) const;
  [[nodiscard]] std::shared_ptr<IndexInfo>
  find_index(const std::string &index_name) const;
  [[nodiscard]] std::shared_ptr<IndexInfo> find_index(oid_t index_oid) const;

  /// Build a serializable snapshot of the current catalog state.
  [[nodiscard]] CatalogManifest snapshot() const;
  /// Persist the manifest if a path is configured (no-op otherwise).
  [[nodiscard]] Status persist() const;
  /// Load the manifest during construction and rebuild heaps/indexes.
  void load_from_manifest();
  /// Keep the manifest's root_page_id current when an index root moves.
  void register_root_listener(const std::shared_ptr<IndexInfo> &info);

  std::shared_ptr<BufferPoolManager> buffer_pool_;
  std::string manifest_path_; ///< Durable manifest path ("" = in-memory only)
  mutable std::shared_mutex mutex_; ///< Guards the maps and next_oid_
  std::unordered_map<std::string, oid_t> table_names_; ///< Name -> OID mapping
  std::unordered_map<oid_t, std::shared_ptr<TableInfo>>
      tables_; ///< OID -> TableInfo
  std::unordered_map<std::string, oid_t> index_names_; ///< Name -> OID mapping
  std::unordered_map<oid_t, std::shared_ptr<IndexInfo>>
      indexes_; ///< OID -> IndexInfo
  oid_t next_oid_ = 1;
};

} // namespace entropy
