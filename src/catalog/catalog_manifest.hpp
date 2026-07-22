#pragma once

/**
 * @file catalog_manifest.hpp
 * @brief Durable, serialized representation of the system catalog
 *
 * The manifest is the on-disk source of truth for catalog metadata. It is
 * written and fsync'd on every DDL (create_table/drop_table/create_index) and
 * reloaded when a Catalog is constructed, so that TableHeap and BPlusTree
 * objects can be rebuilt from their persisted page ids after a restart.
 *
 * Layout (see serialize()):
 *   { next_oid,
 *     tables:  [{ oid, name, schema, first_page_id }],
 *     indexes: [{ oid, name, table_oid, key_column, root_page_id }] }
 */

#include <string>
#include <vector>

#include "catalog/schema.hpp"
#include "common/types.hpp"
#include "entropy/status.hpp"

namespace entropy {

/**
 * @brief Persisted metadata for a single table
 */
struct ManifestTable {
  oid_t oid = INVALID_OID;
  std::string name;
  Schema schema;
  page_id_t first_page_id = INVALID_PAGE_ID;
};

/**
 * @brief Persisted metadata for a single index
 */
struct ManifestIndex {
  oid_t oid = INVALID_OID;
  std::string name;
  oid_t table_oid = INVALID_OID;
  column_id_t key_column = 0;
  page_id_t root_page_id = INVALID_PAGE_ID;
};

/**
 * @brief Serializable snapshot of the whole catalog
 */
struct CatalogManifest {
  oid_t next_oid = 1;
  std::vector<ManifestTable> tables;
  std::vector<ManifestIndex> indexes;

  /**
   * @brief Serialize the manifest to a byte buffer
   */
  [[nodiscard]] std::vector<char> serialize() const;

  /**
   * @brief Parse a manifest from a byte buffer
   * @return Status::Corruption() if the bytes are malformed
   */
  [[nodiscard]] static Status deserialize(const std::vector<char> &bytes,
                                          CatalogManifest *out);

  /**
   * @brief Atomically write the manifest to @p path and fsync it
   *
   * Writes to a temporary file, fsyncs it, renames it over @p path, then
   * fsyncs the containing directory so the rename is durable across a crash.
   */
  [[nodiscard]] Status save(const std::string &path) const;

  /**
   * @brief Load a manifest from @p path
   * @return Status::NotFound() if the file does not exist
   */
  [[nodiscard]] static Status load(const std::string &path,
                                   CatalogManifest *out);
};

} // namespace entropy
