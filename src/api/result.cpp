/**
 * @file result.cpp
 * @brief Result and Value implementations
 */

#include "entropy/result.hpp"

#include <stdexcept>

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Value implementation
// ─────────────────────────────────────────────────────────────────────────────

bool Value::as_bool() const {
    if (auto* v = std::get_if<bool>(&value_)) return *v;
    throw std::runtime_error("Value is not a bool");
}

int32_t Value::as_int32() const {
    if (auto* v = std::get_if<int32_t>(&value_)) return *v;
    throw std::runtime_error("Value is not an int32");
}

int64_t Value::as_int64() const {
    if (auto* v = std::get_if<int64_t>(&value_)) return *v;
    throw std::runtime_error("Value is not an int64");
}

double Value::as_double() const {
    if (auto* v = std::get_if<double>(&value_)) return *v;
    throw std::runtime_error("Value is not a double");
}

const std::string& Value::as_string() const {
    if (auto* v = std::get_if<std::string>(&value_)) return *v;
    throw std::runtime_error("Value is not a string");
}

std::optional<bool> Value::try_bool() const noexcept {
    if (auto* v = std::get_if<bool>(&value_)) return *v;
    return std::nullopt;
}

std::optional<int32_t> Value::try_int32() const noexcept {
    if (auto* v = std::get_if<int32_t>(&value_)) return *v;
    return std::nullopt;
}

std::optional<int64_t> Value::try_int64() const noexcept {
    if (auto* v = std::get_if<int64_t>(&value_)) return *v;
    return std::nullopt;
}

std::optional<double> Value::try_double() const noexcept {
    if (auto* v = std::get_if<double>(&value_)) return *v;
    return std::nullopt;
}

std::optional<std::string_view> Value::try_string() const noexcept {
    if (auto* v = std::get_if<std::string>(&value_)) return *v;
    return std::nullopt;
}

std::string Value::to_string() const {
    if (is_null()) return "NULL";
    if (auto* v = std::get_if<bool>(&value_)) return *v ? "true" : "false";
    if (auto* v = std::get_if<int32_t>(&value_)) return std::to_string(*v);
    if (auto* v = std::get_if<int64_t>(&value_)) return std::to_string(*v);
    if (auto* v = std::get_if<double>(&value_)) return std::to_string(*v);
    if (auto* v = std::get_if<std::string>(&value_)) return *v;
    return "UNKNOWN";
}

// ─────────────────────────────────────────────────────────────────────────────
// Row implementation
// ─────────────────────────────────────────────────────────────────────────────

Row::Row(std::vector<Value> values, std::vector<std::string> column_names)
    : values_(std::move(values)), column_names_(std::move(column_names)) {}

const Value& Row::operator[](size_t index) const {
    return values_.at(index);
}

const Value& Row::operator[](std::string_view name) const {
    for (size_t i = 0; i < column_names_.size(); ++i) {
        if (column_names_[i] == name) {
            return values_[i];
        }
    }
    throw std::runtime_error("Column not found: " + std::string(name));
}

// ─────────────────────────────────────────────────────────────────────────────
// Result implementation
// ─────────────────────────────────────────────────────────────────────────────

Result::Result(Status status) : status_(std::move(status)) {}

Result::Result(std::vector<Row> rows, std::vector<std::string> column_names)
    : status_(Status::Ok()),
      rows_(std::move(rows)),
      column_names_(std::move(column_names)) {}

Result::Result(size_t affected_rows)
    : status_(Status::Ok()), affected_rows_(affected_rows) {}

}  // namespace entropy
