#pragma once

/**
 * @file catalog.hpp
 * @brief System catalog for metadata management
 *
 * The catalog stores metadata about tables and indexes:
 * - TableInfo: name, schema, and TableHeap for data storage
 * - IndexInfo: name, key schema, and index structure (future)
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
// Catalog - System catalog manager
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief System catalog - manages table and index metadata
 *
 * The catalog is responsible for:
 * - Creating and dropping tables
 * - Storing table schemas and TableHeap references
 * - Looking up table information by name or OID
 */
class Catalog {
public:
  /**
   * @brief Construct a catalog with a buffer pool
   * @param buffer_pool Buffer pool manager for creating TableHeaps
   */
  explicit Catalog(std::shared_ptr<BufferPoolManager> buffer_pool);
  ~Catalog() = default;

  // ─────────────────────────────────────────────────────────────────────────
  // Table Management
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Create a new table
   * @param table_name Name of the table
   * @param schema Schema defining the table columns
   * @return Status::Ok() on success, Status::AlreadyExists() if table exists
   */
  [[nodiscard]] Status create_table(const std::string &table_name,
                                    const Schema &schema);

  /**
   * @brief Drop a table
   * @param table_name Name of the table to drop
   * @return Status::Ok() on success, Status::NotFound() if table doesn't exist
   */
  [[nodiscard]] Status drop_table(const std::string &table_name);

  /**
   * @brief Get complete table information by name
   * @param table_name Name of the table
   * @return Pointer to TableInfo, or nullptr if not found
   */
  [[nodiscard]] TableInfo *get_table(const std::string &table_name);

  /**
   * @brief Get complete table information by name (const)
   * @param table_name Name of the table
   * @return Const pointer to TableInfo, or nullptr if not found
   */
  [[nodiscard]] const TableInfo *get_table(const std::string &table_name) const;

  /**
   * @brief Get table schema by name
   * @param table_name Name of the table
   * @return Pointer to Schema, or nullptr if not found
   */
  [[nodiscard]] const Schema *
  get_table_schema(const std::string &table_name) const;

  /**
   * @brief Check if a table exists
   * @param table_name Name of the table
   * @return true if table exists
   */
  [[nodiscard]] bool table_exists(const std::string &table_name) const;

  /**
   * @brief Get table OID by name
   * @param table_name Name of the table
   * @return Table OID, or INVALID_OID if not found
   */
  [[nodiscard]] oid_t get_table_oid(const std::string &table_name) const;

  /**
   * @brief Get all table names
   * @return Vector of table names
   */
  [[nodiscard]] std::vector<std::string> get_table_names() const;

  // ─────────────────────────────────────────────────────────────────────────
  // Accessors
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Get the buffer pool manager
   */
  [[nodiscard]] std::shared_ptr<BufferPoolManager> buffer_pool() const {
    return buffer_pool_;
  }

private:
  std::shared_ptr<BufferPoolManager> buffer_pool_;
  std::unordered_map<std::string, oid_t> table_names_; ///< Name -> OID mapping
  std::unordered_map<oid_t, TableInfo> tables_;        ///< OID -> TableInfo
  oid_t next_oid_ = 1;
};

} // namespace entropy
