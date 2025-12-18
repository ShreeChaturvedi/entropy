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
  explicit Catalog(std::shared_ptr<BufferPoolManager> buffer_pool);
  ~Catalog() = default;

  // ─────────────────────────────────────────────────────────────────────────
  // Table Management
  // ─────────────────────────────────────────────────────────────────────────

  [[nodiscard]] Status create_table(const std::string &table_name,
                                    const Schema &schema);
  [[nodiscard]] Status drop_table(const std::string &table_name);
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
   * @brief Get index info by name
   */
  [[nodiscard]] IndexInfo *get_index(const std::string &index_name);
  [[nodiscard]] const IndexInfo *get_index(const std::string &index_name) const;

  /**
   * @brief Get index for a specific column
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
  std::shared_ptr<BufferPoolManager> buffer_pool_;
  std::unordered_map<std::string, oid_t> table_names_; ///< Name -> OID mapping
  std::unordered_map<oid_t, TableInfo> tables_;        ///< OID -> TableInfo
  std::unordered_map<std::string, oid_t> index_names_; ///< Name -> OID mapping
  std::unordered_map<oid_t, IndexInfo> indexes_;       ///< OID -> IndexInfo
  oid_t next_oid_ = 1;
};

} // namespace entropy
