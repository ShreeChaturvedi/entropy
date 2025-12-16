#pragma once

/**
 * @file tuple.hpp
 * @brief Tuple (row) representation
 */

#include <cstdint>
#include <vector>

#include "common/types.hpp"

namespace entropy {

/**
 * @brief Represents a single tuple (row) in a table
 *
 * A tuple consists of a sequence of values corresponding to the columns
 * of a table schema. Supports serialization for disk storage.
 */
class Tuple {
public:
    Tuple() = default;

    /// Construct from raw data
    Tuple(std::vector<char> data, RID rid);

    /// Get the raw data
    [[nodiscard]] const char* data() const noexcept {
        return data_.data();
    }

    /// Get the tuple size in bytes
    [[nodiscard]] size_t size() const noexcept {
        return data_.size();
    }

    /// Get the record ID
    [[nodiscard]] RID rid() const noexcept { return rid_; }

    /// Set the record ID
    void set_rid(RID rid) noexcept { rid_ = rid; }

    /// Check if tuple is valid
    [[nodiscard]] bool is_valid() const noexcept {
        return !data_.empty();
    }

private:
    std::vector<char> data_;
    RID rid_;
};

}  // namespace entropy
