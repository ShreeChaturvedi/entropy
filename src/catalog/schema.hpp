#pragma once

/**
 * @file schema.hpp
 * @brief Table schema definition
 */

#include <string>
#include <vector>

#include "catalog/column.hpp"

namespace entropy {

/**
 * @brief Table schema - defines the structure of a table
 */
class Schema {
public:
    Schema() = default;
    explicit Schema(std::vector<Column> columns);

    [[nodiscard]] const std::vector<Column>& columns() const noexcept {
        return columns_;
    }

    [[nodiscard]] size_t column_count() const noexcept {
        return columns_.size();
    }

    [[nodiscard]] const Column& column(size_t idx) const {
        return columns_.at(idx);
    }

    /// Get column index by name, returns -1 if not found
    [[nodiscard]] int get_column_index(const std::string& name) const;

private:
    std::vector<Column> columns_;
};

}  // namespace entropy
