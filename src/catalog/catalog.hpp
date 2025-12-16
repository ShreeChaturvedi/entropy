#pragma once

/**
 * @file catalog.hpp
 * @brief System catalog for metadata management
 */

#include <memory>
#include <string>
#include <unordered_map>

#include "catalog/schema.hpp"
#include "common/types.hpp"
#include "entropy/status.hpp"

namespace entropy {

class BufferPoolManager;

/**
 * @brief System catalog - manages table and index metadata
 */
class Catalog {
public:
    explicit Catalog(std::shared_ptr<BufferPoolManager> buffer_pool);
    ~Catalog() = default;

    /// Create a new table
    [[nodiscard]] Status create_table(const std::string& table_name,
                                       const Schema& schema);

    /// Drop a table
    [[nodiscard]] Status drop_table(const std::string& table_name);

    /// Get table schema
    [[nodiscard]] const Schema* get_table_schema(
        const std::string& table_name) const;

    /// Check if table exists
    [[nodiscard]] bool table_exists(const std::string& table_name) const;

    /// Get table OID
    [[nodiscard]] oid_t get_table_oid(const std::string& table_name) const;

private:
    std::shared_ptr<BufferPoolManager> buffer_pool_;
    std::unordered_map<std::string, oid_t> table_names_;
    std::unordered_map<oid_t, Schema> table_schemas_;
    oid_t next_oid_ = 1;
};

}  // namespace entropy
