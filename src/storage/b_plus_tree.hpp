#pragma once

/**
 * @file b_plus_tree.hpp
 * @brief B+ Tree index implementation
 */

#include <memory>
#include <optional>
#include <vector>

#include "common/types.hpp"
#include "entropy/status.hpp"
#include "storage/buffer_pool.hpp"

namespace entropy {

/**
 * @brief B+ Tree index for efficient key-value lookups
 *
 * Supports:
 * - Point lookups: O(log n)
 * - Range scans: O(log n + k) where k is result size
 * - Insertions: O(log n)
 * - Deletions: O(log n)
 */
template <typename KeyType, typename ValueType>
class BPlusTree {
public:
    explicit BPlusTree(std::shared_ptr<BufferPoolManager> buffer_pool,
                       page_id_t root_page_id = INVALID_PAGE_ID);

    ~BPlusTree() = default;

    /// Insert a key-value pair
    [[nodiscard]] Status insert(const KeyType& key, const ValueType& value);

    /// Delete a key
    [[nodiscard]] Status remove(const KeyType& key);

    /// Find a value by key
    [[nodiscard]] std::optional<ValueType> find(const KeyType& key);

    /// Range scan from start_key to end_key
    [[nodiscard]] std::vector<std::pair<KeyType, ValueType>> range_scan(
        const KeyType& start_key, const KeyType& end_key);

    /// Check if tree is empty
    [[nodiscard]] bool is_empty() const;

    /// Get the root page ID
    [[nodiscard]] page_id_t root_page_id() const noexcept {
        return root_page_id_;
    }

private:
    std::shared_ptr<BufferPoolManager> buffer_pool_;
    page_id_t root_page_id_;
};

}  // namespace entropy
