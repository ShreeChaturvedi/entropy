/**
 * @file catalog.cpp
 * @brief Catalog implementation
 */

#include "catalog/catalog.hpp"

namespace entropy {

Catalog::Catalog(std::shared_ptr<BufferPoolManager> buffer_pool)
    : buffer_pool_(std::move(buffer_pool)) {}

Status Catalog::create_table(const std::string& table_name,
                              const Schema& schema) {
    if (table_exists(table_name)) {
        return Status::AlreadyExists("Table already exists: " + table_name);
    }

    oid_t oid = next_oid_++;
    table_names_[table_name] = oid;
    table_schemas_[oid] = schema;

    return Status::Ok();
}

Status Catalog::drop_table(const std::string& table_name) {
    auto it = table_names_.find(table_name);
    if (it == table_names_.end()) {
        return Status::NotFound("Table not found: " + table_name);
    }

    table_schemas_.erase(it->second);
    table_names_.erase(it);

    return Status::Ok();
}

const Schema* Catalog::get_table_schema(const std::string& table_name) const {
    auto it = table_names_.find(table_name);
    if (it == table_names_.end()) {
        return nullptr;
    }

    auto schema_it = table_schemas_.find(it->second);
    if (schema_it == table_schemas_.end()) {
        return nullptr;
    }

    return &schema_it->second;
}

bool Catalog::table_exists(const std::string& table_name) const {
    return table_names_.find(table_name) != table_names_.end();
}

oid_t Catalog::get_table_oid(const std::string& table_name) const {
    auto it = table_names_.find(table_name);
    return it != table_names_.end() ? it->second : INVALID_OID;
}

}  // namespace entropy
