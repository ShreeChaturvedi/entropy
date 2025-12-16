#pragma once

/**
 * @file result.hpp
 * @brief Query result types for Entropy Database Engine
 */

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "entropy/status.hpp"

namespace entropy {

/**
 * @brief Represents a value that can be stored in a database column
 */
class Value {
public:
    using ValueType = std::variant<
        std::monostate,  // NULL
        bool,
        int32_t,
        int64_t,
        double,
        std::string
    >;

    /// Construct a NULL value
    Value() : value_(std::monostate{}) {}

    /// Construct from various types
    explicit Value(bool v) : value_(v) {}
    explicit Value(int32_t v) : value_(v) {}
    explicit Value(int64_t v) : value_(v) {}
    explicit Value(double v) : value_(v) {}
    explicit Value(std::string v) : value_(std::move(v)) {}
    explicit Value(const char* v) : value_(std::string(v)) {}

    /// Check if the value is NULL
    [[nodiscard]] bool is_null() const noexcept {
        return std::holds_alternative<std::monostate>(value_);
    }

    /// Type checking methods
    [[nodiscard]] bool is_bool() const noexcept { return std::holds_alternative<bool>(value_); }
    [[nodiscard]] bool is_int32() const noexcept { return std::holds_alternative<int32_t>(value_); }
    [[nodiscard]] bool is_int64() const noexcept { return std::holds_alternative<int64_t>(value_); }
    [[nodiscard]] bool is_double() const noexcept { return std::holds_alternative<double>(value_); }
    [[nodiscard]] bool is_string() const noexcept { return std::holds_alternative<std::string>(value_); }

    /// Value retrieval methods (throw if wrong type)
    [[nodiscard]] bool as_bool() const;
    [[nodiscard]] int32_t as_int32() const;
    [[nodiscard]] int64_t as_int64() const;
    [[nodiscard]] double as_double() const;
    [[nodiscard]] const std::string& as_string() const;

    /// Safe value retrieval (returns nullopt if wrong type or NULL)
    [[nodiscard]] std::optional<bool> try_bool() const noexcept;
    [[nodiscard]] std::optional<int32_t> try_int32() const noexcept;
    [[nodiscard]] std::optional<int64_t> try_int64() const noexcept;
    [[nodiscard]] std::optional<double> try_double() const noexcept;
    [[nodiscard]] std::optional<std::string_view> try_string() const noexcept;

    /// Convert to string representation
    [[nodiscard]] std::string to_string() const;

private:
    ValueType value_;
};

/**
 * @brief Represents a single row in a query result
 */
class Row {
public:
    Row() = default;
    explicit Row(std::vector<Value> values, std::vector<std::string> column_names);

    /// Get value by column index
    [[nodiscard]] const Value& operator[](size_t index) const;

    /// Get value by column name
    [[nodiscard]] const Value& operator[](std::string_view name) const;

    /// Get number of columns
    [[nodiscard]] size_t size() const noexcept { return values_.size(); }

    /// Check if row is empty
    [[nodiscard]] bool empty() const noexcept { return values_.empty(); }

    /// Iterator support
    [[nodiscard]] auto begin() const noexcept { return values_.begin(); }
    [[nodiscard]] auto end() const noexcept { return values_.end(); }

private:
    std::vector<Value> values_;
    std::vector<std::string> column_names_;
};

/**
 * @brief Result of a query execution
 *
 * Result can represent either:
 * - A success with rows (for SELECT queries)
 * - A success with affected row count (for INSERT/UPDATE/DELETE)
 * - An error
 */
class Result {
public:
    /// Create an error result
    explicit Result(Status status);

    /// Create a success result with rows
    Result(std::vector<Row> rows, std::vector<std::string> column_names);

    /// Create a success result with affected rows count
    explicit Result(size_t affected_rows);

    /// Check if the query succeeded
    [[nodiscard]] bool ok() const noexcept { return status_.ok(); }

    /// Get the status
    [[nodiscard]] const Status& status() const noexcept { return status_; }

    /// Get the rows (empty for non-SELECT queries)
    [[nodiscard]] const std::vector<Row>& rows() const noexcept { return rows_; }

    /// Get the column names
    [[nodiscard]] const std::vector<std::string>& column_names() const noexcept { return column_names_; }

    /// Get the number of affected rows (for INSERT/UPDATE/DELETE)
    [[nodiscard]] size_t affected_rows() const noexcept { return affected_rows_; }

    /// Get the number of rows returned
    [[nodiscard]] size_t row_count() const noexcept { return rows_.size(); }

    /// Check if result has rows
    [[nodiscard]] bool has_rows() const noexcept { return !rows_.empty(); }

    /// Iterator support for range-based for loops
    [[nodiscard]] auto begin() const noexcept { return rows_.begin(); }
    [[nodiscard]] auto end() const noexcept { return rows_.end(); }

private:
    Status status_;
    std::vector<Row> rows_;
    std::vector<std::string> column_names_;
    size_t affected_rows_ = 0;
};

}  // namespace entropy
