/**
 * @file schema.cpp
 * @brief Schema implementation
 */

#include "catalog/schema.hpp"

namespace entropy {

Schema::Schema(std::vector<Column> columns) : columns_(std::move(columns)) {}

int Schema::get_column_index(const std::string& name) const {
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i].name() == name) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

}  // namespace entropy
