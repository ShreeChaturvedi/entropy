#pragma once

/**
 * @file tuple.hpp
 * @brief Tuple (row) representation with serialization
 *
 * Tuple Layout:
 * +------------------+
 * | Null Bitmap      |  Ceil(num_columns / 8) bytes
 * +------------------+
 * | Fixed-size cols  |  Stored in column order
 * +------------------+
 * | Variable-length  |  Length (2 bytes) + data for each VARCHAR
 * +------------------+
 */

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "catalog/schema.hpp"
#include "common/types.hpp"

namespace entropy {

// Forward declaration
class Schema;

/**
 * @brief Represents a value that can be stored in a tuple column
 */
class TupleValue {
public:
    using ValueType = std::variant<
        std::monostate,  // NULL
        bool,
        int8_t,          // TINYINT
        int16_t,         // SMALLINT
        int32_t,         // INTEGER
        int64_t,         // BIGINT
        float,           // FLOAT
        double,          // DOUBLE
        std::string      // VARCHAR
    >;

    /// Construct a NULL value
    TupleValue() : value_(std::monostate{}) {}

    /// Construct from various types
    explicit TupleValue(bool v) : value_(v) {}
    explicit TupleValue(int8_t v) : value_(v) {}
    explicit TupleValue(int16_t v) : value_(v) {}
    explicit TupleValue(int32_t v) : value_(v) {}
    explicit TupleValue(int64_t v) : value_(v) {}
    explicit TupleValue(float v) : value_(v) {}
    explicit TupleValue(double v) : value_(v) {}
    explicit TupleValue(std::string v) : value_(std::move(v)) {}
    explicit TupleValue(const char* v) : value_(std::string(v)) {}

    /// Check if the value is NULL
    [[nodiscard]] bool is_null() const noexcept {
        return std::holds_alternative<std::monostate>(value_);
    }

    /// Type checking methods
    [[nodiscard]] bool is_bool() const noexcept { return std::holds_alternative<bool>(value_); }
    [[nodiscard]] bool is_tinyint() const noexcept { return std::holds_alternative<int8_t>(value_); }
    [[nodiscard]] bool is_smallint() const noexcept { return std::holds_alternative<int16_t>(value_); }
    [[nodiscard]] bool is_integer() const noexcept { return std::holds_alternative<int32_t>(value_); }
    [[nodiscard]] bool is_bigint() const noexcept { return std::holds_alternative<int64_t>(value_); }
    [[nodiscard]] bool is_float() const noexcept { return std::holds_alternative<float>(value_); }
    [[nodiscard]] bool is_double() const noexcept { return std::holds_alternative<double>(value_); }
    [[nodiscard]] bool is_string() const noexcept { return std::holds_alternative<std::string>(value_); }

    /// Value retrieval methods
    [[nodiscard]] bool as_bool() const { return std::get<bool>(value_); }
    [[nodiscard]] int8_t as_tinyint() const { return std::get<int8_t>(value_); }
    [[nodiscard]] int16_t as_smallint() const { return std::get<int16_t>(value_); }
    [[nodiscard]] int32_t as_integer() const { return std::get<int32_t>(value_); }
    [[nodiscard]] int64_t as_bigint() const { return std::get<int64_t>(value_); }
    [[nodiscard]] float as_float() const { return std::get<float>(value_); }
    [[nodiscard]] double as_double() const { return std::get<double>(value_); }
    [[nodiscard]] const std::string& as_string() const { return std::get<std::string>(value_); }

    /// Get the underlying variant
    [[nodiscard]] const ValueType& value() const noexcept { return value_; }

    /// Create null value
    static TupleValue null() { return TupleValue(); }

    /// Create from type and raw bytes
    static TupleValue from_bytes(TypeId type, const char* data, size_t length);

    /// Serialize to bytes
    [[nodiscard]] std::vector<char> to_bytes(TypeId type) const;

    /// Compare values
    bool operator==(const TupleValue& other) const { return value_ == other.value_; }
    bool operator!=(const TupleValue& other) const { return value_ != other.value_; }

private:
    ValueType value_;
};

/**
 * @brief Represents a single tuple (row) in a table
 *
 * A tuple consists of a sequence of values corresponding to the columns
 * of a table schema. Supports serialization for disk storage.
 */
class Tuple {
public:
    Tuple() = default;

    /**
     * @brief Construct from raw serialized data
     * @param data Serialized tuple data
     * @param rid Record ID
     */
    Tuple(std::vector<char> data, RID rid);

    /**
     * @brief Construct from values and schema
     * @param values Column values (must match schema)
     * @param schema Table schema
     */
    Tuple(const std::vector<TupleValue>& values, const Schema& schema);

    // ─────────────────────────────────────────────────────────────────────────
    // Data Access
    // ─────────────────────────────────────────────────────────────────────────

    /// Get the raw serialized data
    [[nodiscard]] const char* data() const noexcept {
        return data_.data();
    }

    /// Get mutable data pointer
    [[nodiscard]] char* data_mut() noexcept {
        return data_.data();
    }

    /// Get the tuple size in bytes
    [[nodiscard]] uint32_t size() const noexcept {
        return static_cast<uint32_t>(data_.size());
    }

    /// Get data as span
    [[nodiscard]] std::span<const char> as_span() const noexcept {
        return std::span<const char>(data_);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Value Access
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Get a value by column index
     * @param schema Table schema
     * @param col_idx Column index
     * @return The value at the specified column
     */
    [[nodiscard]] TupleValue get_value(const Schema& schema, uint32_t col_idx) const;

    /**
     * @brief Check if a column is NULL
     * @param col_idx Column index
     * @return true if the column is NULL
     */
    [[nodiscard]] bool is_null(uint32_t col_idx) const;

    // ─────────────────────────────────────────────────────────────────────────
    // Record ID
    // ─────────────────────────────────────────────────────────────────────────

    /// Get the record ID
    [[nodiscard]] RID rid() const noexcept { return rid_; }

    /// Set the record ID
    void set_rid(RID rid) noexcept { rid_ = rid; }

    // ─────────────────────────────────────────────────────────────────────────
    // Validation
    // ─────────────────────────────────────────────────────────────────────────

    /// Check if tuple is valid (has data)
    [[nodiscard]] bool is_valid() const noexcept {
        return !data_.empty();
    }

    /// Check if tuple data is empty
    [[nodiscard]] bool is_empty() const noexcept {
        return data_.empty();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Serialization Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Get the size of null bitmap for given column count
     */
    [[nodiscard]] static uint32_t null_bitmap_size(uint32_t num_columns) noexcept {
        return (num_columns + 7) / 8;
    }

    /**
     * @brief Calculate the serialized size for a tuple
     * @param values Column values
     * @param schema Table schema
     */
    [[nodiscard]] static uint32_t serialized_size(
        const std::vector<TupleValue>& values,
        const Schema& schema
    );

private:
    /// Get offset of a column in the serialized data
    [[nodiscard]] uint32_t get_column_offset(const Schema& schema, uint32_t col_idx) const;

    /// Set a bit in the null bitmap
    void set_null_bit(uint32_t col_idx, bool is_null);

    std::vector<char> data_;
    RID rid_;
};

}  // namespace entropy
