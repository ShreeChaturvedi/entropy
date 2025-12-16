#pragma once

/**
 * @file hash_index.hpp
 * @brief Hash index for O(1) equality lookups
 */

#include <memory>
#include <optional>

#include "common/types.hpp"
#include "entropy/status.hpp"
#include "storage/buffer_pool.hpp"

namespace entropy {

/**
 * @brief Extendible Hash Index
 */
template <typename KeyType, typename ValueType>
class HashIndex {
public:
    explicit HashIndex(std::shared_ptr<BufferPoolManager> buffer_pool);
    ~HashIndex() = default;

    /// Insert a key-value pair
    [[nodiscard]] Status insert(const KeyType& key, const ValueType& value);

    /// Delete a key
    [[nodiscard]] Status remove(const KeyType& key);

    /// Find a value by key
    [[nodiscard]] std::optional<ValueType> find(const KeyType& key);

private:
    std::shared_ptr<BufferPoolManager> buffer_pool_;
};

}  // namespace entropy
