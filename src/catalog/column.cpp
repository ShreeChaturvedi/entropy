/**
 * @file column.cpp
 * @brief Column implementation
 */

#include "catalog/column.hpp"

namespace entropy {

Column::Column(std::string name, TypeId type, size_t length)
    : name_(std::move(name)), type_(type), length_(length) {
    // For fixed-size types, set length from type
    if (length_ == 0 && !is_variable_length(type)) {
        length_ = type_size(type);
    }
}

}  // namespace entropy
