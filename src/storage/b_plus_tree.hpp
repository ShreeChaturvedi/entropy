#pragma once

/**
 * @file b_plus_tree.hpp
 * @brief B+ Tree index implementation
 *
 * A B+ Tree is a self-balancing tree data structure that maintains sorted data
 * and allows searches, sequential access, insertions, and deletions in O(log n).
 *
 * Properties:
 * - All values are stored in leaf nodes
 * - Internal nodes only store keys for navigation
 * - Leaf nodes are linked for efficient range scans
 * - Tree is always balanced (all leaves at same depth)
 */

#include <memory>
#include <optional>
#include <vector>

#include "common/types.hpp"
#include "entropy/status.hpp"
#include "storage/b_plus_tree_page.hpp"
#include "storage/buffer_pool.hpp"

namespace entropy {

// Forward declaration
class BPlusTreeIterator;

/**
 * @brief B+ Tree index for efficient key-value lookups
 *
 * Uses int64_t keys and RID values (pointing to actual tuples).
 *
 * Supports:
 * - Point lookups: O(log n)
 * - Range scans: O(log n + k) where k is result size
 * - Insertions: O(log n) amortized
 * - Deletions: O(log n) amortized
 */
class BPlusTree {
public:
    using KeyType = BPTreeKey;
    using ValueType = BPTreeValue;

    /**
     * @brief Construct a new B+ Tree
     * @param buffer_pool Buffer pool manager for page access
     * @param root_page_id Existing root page ID, or INVALID_PAGE_ID for new tree
     */
    explicit BPlusTree(std::shared_ptr<BufferPoolManager> buffer_pool,
                       page_id_t root_page_id = INVALID_PAGE_ID);

    ~BPlusTree() = default;

    // ─────────────────────────────────────────────────────────────────────────
    // Core Operations
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Insert a key-value pair
     * @param key The key to insert
     * @param value The value (RID) associated with the key
     * @return Status::Ok() on success, error on failure or duplicate key
     */
    [[nodiscard]] Status insert(KeyType key, const ValueType& value);

    /**
     * @brief Delete a key from the tree
     * @param key The key to delete
     * @return Status::Ok() on success, Status::NotFound() if key doesn't exist
     */
    [[nodiscard]] Status remove(KeyType key);

    /**
     * @brief Find a value by key
     * @param key The key to search for
     * @return The value if found, nullopt otherwise
     */
    [[nodiscard]] std::optional<ValueType> find(KeyType key);

    /**
     * @brief Get all key-value pairs in a range [start_key, end_key]
     * @param start_key Start of range (inclusive)
     * @param end_key End of range (inclusive)
     * @return Vector of key-value pairs in the range
     */
    [[nodiscard]] std::vector<std::pair<KeyType, ValueType>> range_scan(
        KeyType start_key, KeyType end_key);

    // ─────────────────────────────────────────────────────────────────────────
    // Iteration
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Get iterator to first element >= key
     */
    [[nodiscard]] BPlusTreeIterator lower_bound(KeyType key);

    /**
     * @brief Get iterator to the beginning (smallest key)
     */
    [[nodiscard]] BPlusTreeIterator begin();

    /**
     * @brief Get iterator to the end
     */
    [[nodiscard]] BPlusTreeIterator end();

    // ─────────────────────────────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Check if tree is empty
     */
    [[nodiscard]] bool is_empty() const;

    /**
     * @brief Get the root page ID
     */
    [[nodiscard]] page_id_t root_page_id() const noexcept {
        return root_page_id_;
    }

    /**
     * @brief Get the buffer pool manager
     */
    [[nodiscard]] std::shared_ptr<BufferPoolManager> buffer_pool() const {
        return buffer_pool_;
    }

private:
    // ─────────────────────────────────────────────────────────────────────────
    // Helper Methods
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Find the leaf page that should contain the key
     * @param key The key to search for
     * @return Page ID of the leaf, or INVALID_PAGE_ID if tree is empty
     */
    [[nodiscard]] page_id_t find_leaf(KeyType key);

    /**
     * @brief Create a new leaf page
     * @return The new page, or nullptr on failure
     */
    [[nodiscard]] Page* create_leaf_page(page_id_t* page_id);

    /**
     * @brief Create a new internal page
     * @return The new page, or nullptr on failure
     */
    [[nodiscard]] Page* create_internal_page(page_id_t* page_id);

    /**
     * @brief Insert into a leaf, splitting if necessary
     * @param leaf_page The leaf page
     * @param key Key to insert
     * @param value Value to insert
     * @return Status and optional (split_key, new_page_id) if split occurred
     */
    struct InsertResult {
        Status status;
        bool did_split = false;
        KeyType split_key = 0;
        page_id_t new_page_id = INVALID_PAGE_ID;
    };
    [[nodiscard]] InsertResult insert_into_leaf(Page* leaf_page, KeyType key,
                                                 const ValueType& value);

    /**
     * @brief Insert a new key into parent after a child split
     */
    [[nodiscard]] Status insert_into_parent(Page* old_page, KeyType key,
                                             page_id_t new_page_id);

    /**
     * @brief Update parent pointers for all children of an internal node
     */
    void update_children_parent(page_id_t parent_id, BPTreeInternalPage* internal);

    std::shared_ptr<BufferPoolManager> buffer_pool_;
    page_id_t root_page_id_;

    // Maximum sizes for nodes (computed once)
    uint32_t leaf_max_size_;
    uint32_t internal_max_size_;
};

/**
 * @brief Iterator for B+ Tree range scans
 *
 * Iterates through leaf nodes using sibling pointers.
 */
class BPlusTreeIterator {
public:
    using KeyType = BPTreeKey;
    using ValueType = BPTreeValue;

    /**
     * @brief Construct an end iterator
     */
    BPlusTreeIterator() = default;

    /**
     * @brief Construct an iterator at a specific position
     */
    BPlusTreeIterator(BPlusTree* tree, page_id_t leaf_page_id, uint32_t index);

    /**
     * @brief Get current key
     */
    [[nodiscard]] KeyType key() const;

    /**
     * @brief Get current value
     */
    [[nodiscard]] ValueType value() const;

    /**
     * @brief Get key-value pair
     */
    [[nodiscard]] std::pair<KeyType, ValueType> operator*() const;

    /**
     * @brief Pre-increment - move to next entry
     */
    BPlusTreeIterator& operator++();

    /**
     * @brief Post-increment
     */
    BPlusTreeIterator operator++(int);

    /**
     * @brief Check if at end
     */
    [[nodiscard]] bool is_end() const {
        return leaf_page_id_ == INVALID_PAGE_ID;
    }

    /**
     * @brief Equality comparison
     */
    bool operator==(const BPlusTreeIterator& other) const {
        return leaf_page_id_ == other.leaf_page_id_ && index_ == other.index_;
    }

    bool operator!=(const BPlusTreeIterator& other) const {
        return !(*this == other);
    }

private:
    BPlusTree* tree_ = nullptr;
    page_id_t leaf_page_id_ = INVALID_PAGE_ID;
    uint32_t index_ = 0;
};

}  // namespace entropy
