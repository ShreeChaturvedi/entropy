#pragma once

/**
 * @file column.hpp
 * @brief Column metadata
 */

#include <string>

#include "common/types.hpp"

namespace entropy {

/**
 * @brief Column definition in a table schema
 */
class Column {
public:
    Column(std::string name, TypeId type, size_t length = 0);

    [[nodiscard]] const std::string& name() const noexcept { return name_; }
    [[nodiscard]] TypeId type() const noexcept { return type_; }
    [[nodiscard]] size_t length() const noexcept { return length_; }
    [[nodiscard]] bool is_nullable() const noexcept { return nullable_; }

    void set_nullable(bool nullable) noexcept { nullable_ = nullable; }

private:
    std::string name_;
    TypeId type_;
    size_t length_;
    bool nullable_ = true;
};

}  // namespace entropy
