/**
 * @file tuple.cpp
 * @brief Tuple serialization implementation
 */

#include "storage/tuple.hpp"

#include <cstring>
#include <stdexcept>

#include "catalog/schema.hpp"
#include "common/macros.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// TupleValue Implementation
// ─────────────────────────────────────────────────────────────────────────────

TupleValue TupleValue::from_bytes(TypeId type, const char* data, size_t length) {
    switch (type) {
        case TypeId::BOOLEAN: {
            if (length < 1) return TupleValue::null();
            bool val = (*data != 0);
            return TupleValue(val);
        }
        case TypeId::TINYINT: {
            if (length < 1) return TupleValue::null();
            int8_t val;
            std::memcpy(&val, data, sizeof(val));
            return TupleValue(val);
        }
        case TypeId::SMALLINT: {
            if (length < 2) return TupleValue::null();
            int16_t val;
            std::memcpy(&val, data, sizeof(val));
            return TupleValue(val);
        }
        case TypeId::INTEGER: {
            if (length < 4) return TupleValue::null();
            int32_t val;
            std::memcpy(&val, data, sizeof(val));
            return TupleValue(val);
        }
        case TypeId::BIGINT:
        case TypeId::TIMESTAMP: {
            if (length < 8) return TupleValue::null();
            int64_t val;
            std::memcpy(&val, data, sizeof(val));
            return TupleValue(val);
        }
        case TypeId::FLOAT: {
            if (length < 4) return TupleValue::null();
            float val;
            std::memcpy(&val, data, sizeof(val));
            return TupleValue(val);
        }
        case TypeId::DOUBLE: {
            if (length < 8) return TupleValue::null();
            double val;
            std::memcpy(&val, data, sizeof(val));
            return TupleValue(val);
        }
        case TypeId::VARCHAR: {
            // Length is the actual string length (not including length prefix)
            return TupleValue(std::string(data, length));
        }
        default:
            return TupleValue::null();
    }
}

std::vector<char> TupleValue::to_bytes(TypeId type) const {
    std::vector<char> result;

    if (is_null()) {
        // Return empty for null - actual nullness is tracked in bitmap
        return result;
    }

    switch (type) {
        case TypeId::BOOLEAN: {
            result.resize(1);
            result[0] = as_bool() ? 1 : 0;
            break;
        }
        case TypeId::TINYINT: {
            result.resize(1);
            int8_t val = as_tinyint();
            std::memcpy(result.data(), &val, sizeof(val));
            break;
        }
        case TypeId::SMALLINT: {
            result.resize(2);
            int16_t val = as_smallint();
            std::memcpy(result.data(), &val, sizeof(val));
            break;
        }
        case TypeId::INTEGER: {
            result.resize(4);
            int32_t val = as_integer();
            std::memcpy(result.data(), &val, sizeof(val));
            break;
        }
        case TypeId::BIGINT:
        case TypeId::TIMESTAMP: {
            result.resize(8);
            int64_t val = as_bigint();
            std::memcpy(result.data(), &val, sizeof(val));
            break;
        }
        case TypeId::FLOAT: {
            result.resize(4);
            float val = as_float();
            std::memcpy(result.data(), &val, sizeof(val));
            break;
        }
        case TypeId::DOUBLE: {
            result.resize(8);
            double val = as_double();
            std::memcpy(result.data(), &val, sizeof(val));
            break;
        }
        case TypeId::VARCHAR: {
            const std::string& str = as_string();
            // Length prefix (2 bytes) + string data
            result.resize(2 + str.size());
            uint16_t len = static_cast<uint16_t>(str.size());
            std::memcpy(result.data(), &len, sizeof(len));
            std::memcpy(result.data() + 2, str.data(), str.size());
            break;
        }
        default:
            break;
    }

    return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// Tuple Implementation
// ─────────────────────────────────────────────────────────────────────────────

Tuple::Tuple(std::vector<char> data, RID rid)
    : data_(std::move(data)), rid_(rid) {}

Tuple::Tuple(const std::vector<TupleValue>& values, const Schema& schema) {
    // Calculate total size
    uint32_t total_size = serialized_size(values, schema);
    data_.resize(total_size);

    uint32_t num_cols = static_cast<uint32_t>(schema.column_count());
    uint32_t bitmap_size = null_bitmap_size(num_cols);

    // Initialize null bitmap to all zeros (not null)
    std::memset(data_.data(), 0, bitmap_size);

    // Current write position (after null bitmap)
    uint32_t offset = bitmap_size;

    // First pass: write fixed-size columns and set null bits
    for (uint32_t i = 0; i < num_cols; ++i) {
        const Column& col = schema.column(i);
        const TupleValue& val = values[i];

        if (val.is_null()) {
            set_null_bit(i, true);
            // Still need to advance offset for fixed-size columns
            if (!is_variable_length(col.type())) {
                offset += static_cast<uint32_t>(type_size(col.type()));
            }
        } else {
            if (!is_variable_length(col.type())) {
                // Fixed-size column - write directly
                std::vector<char> bytes = val.to_bytes(col.type());
                std::memcpy(data_.data() + offset, bytes.data(), bytes.size());
                offset += static_cast<uint32_t>(bytes.size());
            }
        }
    }

    // Second pass: write variable-length columns
    for (uint32_t i = 0; i < num_cols; ++i) {
        const Column& col = schema.column(i);
        const TupleValue& val = values[i];

        if (is_variable_length(col.type())) {
            if (val.is_null()) {
                // Write zero length for null varchar
                uint16_t len = 0;
                std::memcpy(data_.data() + offset, &len, sizeof(len));
                offset += sizeof(len);
            } else {
                // Write length + data
                std::vector<char> bytes = val.to_bytes(col.type());
                std::memcpy(data_.data() + offset, bytes.data(), bytes.size());
                offset += static_cast<uint32_t>(bytes.size());
            }
        }
    }
}

TupleValue Tuple::get_value(const Schema& schema, uint32_t col_idx) const {
    if (col_idx >= schema.column_count()) {
        throw std::out_of_range("Column index out of range");
    }

    // Check if null
    if (is_null(col_idx)) {
        return TupleValue::null();
    }

    const Column& col = schema.column(col_idx);
    uint32_t offset = get_column_offset(schema, col_idx);

    if (is_variable_length(col.type())) {
        // Read length prefix
        uint16_t len;
        std::memcpy(&len, data_.data() + offset, sizeof(len));
        offset += sizeof(len);

        return TupleValue::from_bytes(col.type(), data_.data() + offset, len);
    } else {
        size_t size = type_size(col.type());
        return TupleValue::from_bytes(col.type(), data_.data() + offset, size);
    }
}

bool Tuple::is_null(uint32_t col_idx) const {
    if (data_.empty()) {
        return true;
    }

    uint32_t byte_idx = col_idx / 8;
    uint32_t bit_idx = col_idx % 8;

    if (byte_idx >= data_.size()) {
        return true;
    }

    return (data_[byte_idx] & (1 << bit_idx)) != 0;
}

uint32_t Tuple::serialized_size(const std::vector<TupleValue>& values,
                                 const Schema& schema) {
    uint32_t num_cols = static_cast<uint32_t>(schema.column_count());
    uint32_t size = null_bitmap_size(num_cols);

    for (uint32_t i = 0; i < num_cols; ++i) {
        const Column& col = schema.column(i);
        const TupleValue& val = values[i];

        if (is_variable_length(col.type())) {
            // Length prefix (2 bytes) + data
            size += 2;
            if (!val.is_null() && val.is_string()) {
                size += static_cast<uint32_t>(val.as_string().size());
            }
        } else {
            // Fixed-size column
            size += static_cast<uint32_t>(type_size(col.type()));
        }
    }

    return size;
}

uint32_t Tuple::get_column_offset(const Schema& schema, uint32_t col_idx) const {
    uint32_t num_cols = static_cast<uint32_t>(schema.column_count());
    uint32_t offset = null_bitmap_size(num_cols);

    // Calculate offset for fixed-size columns first
    for (uint32_t i = 0; i < col_idx; ++i) {
        const Column& col = schema.column(i);
        if (!is_variable_length(col.type())) {
            offset += static_cast<uint32_t>(type_size(col.type()));
        }
    }

    // If target column is fixed-size, we're done
    if (!is_variable_length(schema.column(col_idx).type())) {
        return offset;
    }

    // For variable-length column, continue past all fixed columns
    for (uint32_t i = col_idx; i < num_cols; ++i) {
        const Column& col = schema.column(i);
        if (!is_variable_length(col.type())) {
            offset += static_cast<uint32_t>(type_size(col.type()));
        }
    }

    // Now scan through variable-length columns before target
    for (uint32_t i = 0; i < col_idx; ++i) {
        const Column& col = schema.column(i);
        if (is_variable_length(col.type())) {
            // Read length and skip
            uint16_t len;
            std::memcpy(&len, data_.data() + offset, sizeof(len));
            offset += static_cast<uint32_t>(sizeof(len)) + len;
        }
    }

    return offset;
}

void Tuple::set_null_bit(uint32_t col_idx, bool is_null_val) {
    uint32_t byte_idx = col_idx / 8;
    uint32_t bit_idx = col_idx % 8;

    if (byte_idx >= data_.size()) {
        return;
    }

    if (is_null_val) {
        data_[byte_idx] |= static_cast<char>(1 << bit_idx);
    } else {
        data_[byte_idx] &= static_cast<char>(~(1 << bit_idx));
    }
}

}  // namespace entropy
