#pragma once

/**
 * @file b_plus_tree_page.hpp
 * @brief B+ Tree page structures
 *
 * B+ Tree Node Layout:
 *
 * HEADER (common to both internal and leaf nodes):
 * +------------------+
 * | page_type        |  1 byte  (0 = invalid, 1 = internal, 2 = leaf)
 * | num_keys         |  4 bytes (number of keys in this node)
 * | max_size         |  4 bytes (maximum keys this node can hold)
 * | parent_page_id   |  4 bytes (parent node page ID)
 * | reserved         |  3 bytes (for alignment)
 * +------------------+  Total: 16 bytes
 *
 * INTERNAL NODE (after header):
 * +------------------+
 * | child_0          |  4 bytes (page_id_t)
 * | key_0            |  8 bytes (KeyType)
 * | child_1          |  4 bytes
 * | key_1            |  8 bytes
 * | ...              |
 * | key_{n-1}        |  8 bytes
 * | child_n          |  4 bytes
 * +------------------+
 *
 * Note: Internal node has n keys and n+1 children
 * Keys are sorted: child_i contains keys < key_i
 *
 * LEAF NODE (after header):
 * +------------------+
 * | next_leaf_id     |  4 bytes (sibling pointer for range scans)
 * | prev_leaf_id     |  4 bytes (doubly linked for reverse scans)
 * | key_0            |  8 bytes (KeyType)
 * | value_0          |  8 bytes (RID - page_id + slot_id)
 * | key_1            |  8 bytes
 * | value_1          |  8 bytes
 * | ...              |
 * +------------------+
 */

#include <cstdint>
#include <cstring>
#include <utility>

#include "common/config.hpp"
#include "common/types.hpp"
#include "storage/page.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// B+ Tree Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Type for keys in the B+ tree (using int64_t for flexibility)
using BPTreeKey = int64_t;

/// Type for values in the B+ tree (RID to actual tuple)
using BPTreeValue = RID;

/// B+ Tree page types
enum class BPTreePageType : uint8_t { INVALID = 0, INTERNAL = 1, LEAF = 2 };

// ─────────────────────────────────────────────────────────────────────────────
// B+ Tree Page Header
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Common header for all B+ tree pages
 */
struct BPTreeHeader {
  BPTreePageType page_type = BPTreePageType::INVALID;
  uint8_t reserved1[3] = {}; // Alignment padding
  uint32_t num_keys = 0;     // Number of keys stored
  uint32_t max_size = 0;     // Maximum keys this node can hold
  page_id_t parent_page_id = INVALID_PAGE_ID;
};

static_assert(sizeof(BPTreeHeader) == 16, "BPTreeHeader must be 16 bytes");

// ─────────────────────────────────────────────────────────────────────────────
// B+ Tree Page Base Class
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Base class for B+ tree pages
 *
 * Provides common functionality for both internal and leaf nodes.
 */
class BPTreePage {
public:
  /// Header size in bytes
  static constexpr size_t kHeaderSize = sizeof(BPTreeHeader);

  /// Construct from raw page
  explicit BPTreePage(Page *page) : page_(page) {}

  // ─────────────────────────────────────────────────────────────────────────
  // Header Access
  // ─────────────────────────────────────────────────────────────────────────

  [[nodiscard]] BPTreePageType page_type() const { return header()->page_type; }

  [[nodiscard]] uint32_t num_keys() const { return header()->num_keys; }

  [[nodiscard]] uint32_t max_size() const { return header()->max_size; }

  [[nodiscard]] page_id_t parent_page_id() const {
    return header()->parent_page_id;
  }

  [[nodiscard]] bool is_leaf() const {
    return page_type() == BPTreePageType::LEAF;
  }

  [[nodiscard]] bool is_internal() const {
    return page_type() == BPTreePageType::INTERNAL;
  }

  [[nodiscard]] bool is_root() const {
    return parent_page_id() == INVALID_PAGE_ID;
  }

  [[nodiscard]] bool is_empty() const { return num_keys() == 0; }

  [[nodiscard]] bool is_full() const { return num_keys() >= max_size(); }

  /// Minimum keys for non-root nodes (max_size / 2)
  [[nodiscard]] uint32_t min_size() const { return max_size() / 2; }

  [[nodiscard]] bool is_underflow() const {
    return !is_root() && num_keys() < min_size();
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Modifiers
  // ─────────────────────────────────────────────────────────────────────────

  void set_page_type(BPTreePageType type) {
    header_mut()->page_type = type;
    page_->set_dirty(true);
  }

  void set_num_keys(uint32_t n) {
    header_mut()->num_keys = n;
    page_->set_dirty(true);
  }

  void set_max_size(uint32_t n) {
    header_mut()->max_size = n;
    page_->set_dirty(true);
  }

  void set_parent_page_id(page_id_t id) {
    header_mut()->parent_page_id = id;
    page_->set_dirty(true);
  }

  void increment_num_keys() {
    header_mut()->num_keys++;
    page_->set_dirty(true);
  }

  void decrement_num_keys() {
    if (header()->num_keys > 0) {
      header_mut()->num_keys--;
      page_->set_dirty(true);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Page Access
  // ─────────────────────────────────────────────────────────────────────────

  [[nodiscard]] page_id_t page_id() const { return page_->page_id(); }

  [[nodiscard]] Page *page() { return page_; }
  [[nodiscard]] const Page *page() const { return page_; }

protected:
  [[nodiscard]] const BPTreeHeader *header() const {
    return reinterpret_cast<const BPTreeHeader *>(page_->data() +
                                                  config::kPageHeaderSize);
  }

  [[nodiscard]] BPTreeHeader *header_mut() {
    return reinterpret_cast<BPTreeHeader *>(page_->data() +
                                            config::kPageHeaderSize);
  }

  /// Get pointer to data area (after header)
  [[nodiscard]] char *data_area() {
    return page_->data() + config::kPageHeaderSize + kHeaderSize;
  }

  [[nodiscard]] const char *data_area() const {
    return page_->data() + config::kPageHeaderSize + kHeaderSize;
  }

  Page *page_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Internal Node Page
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Internal node in B+ tree
 *
 * Stores keys and child page pointers.
 * For n keys, there are n+1 children.
 *
 * Layout: child_0 | key_0 | child_1 | key_1 | ... | key_{n-1} | child_n
 */
class BPTreeInternalPage : public BPTreePage {
public:
  /// Size of each key-child pair (except first child)
  static constexpr size_t kPairSize = sizeof(BPTreeKey) + sizeof(page_id_t);

  explicit BPTreeInternalPage(Page *page) : BPTreePage(page) {}

  /**
   * @brief Initialize as internal node
   * @param max_keys Maximum number of keys this node can hold
   */
  void init(uint32_t max_keys) {
    set_page_type(BPTreePageType::INTERNAL);
    set_num_keys(0);
    set_max_size(max_keys);
    set_parent_page_id(INVALID_PAGE_ID);
  }

  /**
   * @brief Calculate max keys for given page size
   */
  static uint32_t compute_max_size() {
    // Available space = page_size - page_header - btree_header - first_child
    size_t available = config::kDefaultPageSize - config::kPageHeaderSize -
                       kHeaderSize - sizeof(page_id_t);
    // Each entry is key + child_id
    return static_cast<uint32_t>(available / kPairSize);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Key Access
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Get key at index
   */
  [[nodiscard]] BPTreeKey key_at(uint32_t index) const {
    if (index >= num_keys())
      return 0;
    BPTreeKey key;
    std::memcpy(&key, key_ptr(index), sizeof(key));
    return key;
  }

  /**
   * @brief Set key at index
   */
  void set_key_at(uint32_t index, BPTreeKey key) {
    if (index < max_size()) {
      std::memcpy(key_ptr_mut(index), &key, sizeof(key));
      page_->set_dirty(true);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Child Access
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Get child page ID at index
   */
  [[nodiscard]] page_id_t child_at(uint32_t index) const {
    if (index > num_keys())
      return INVALID_PAGE_ID;
    page_id_t child;
    std::memcpy(&child, child_ptr(index), sizeof(child));
    return child;
  }

  /**
   * @brief Set child page ID at index
   */
  void set_child_at(uint32_t index, page_id_t child_id) {
    if (index <= max_size()) {
      std::memcpy(child_ptr_mut(index), &child_id, sizeof(child_id));
      page_->set_dirty(true);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Search
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Find the child index for a given key
   *
   * Returns the index of the child that should contain the key.
   * Uses binary search.
   */
  [[nodiscard]] uint32_t find_child_index(BPTreeKey key) const {
    uint32_t n = num_keys();
    if (n == 0)
      return 0;

    // Binary search for the first key > key
    uint32_t left = 0;
    uint32_t right = n;

    while (left < right) {
      uint32_t mid = left + (right - left) / 2;
      if (key_at(mid) <= key) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    return left;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Insertion
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Insert a key and right child after the given child index
   *
   * @param index The index where to insert (key goes at index, child goes at
   * index+1)
   * @param key The key to insert
   * @param right_child The right child page ID
   * @return true if successful
   */
  bool insert_at(uint32_t index, BPTreeKey key, page_id_t right_child) {
    if (is_full())
      return false;

    uint32_t n = num_keys();

    // Shift keys and children to make room
    for (uint32_t i = n; i > index; --i) {
      set_key_at(i, key_at(i - 1));
      set_child_at(i + 1, child_at(i));
    }

    // Insert new key and child
    set_key_at(index, key);
    set_child_at(index + 1, right_child);
    increment_num_keys();

    return true;
  }

  /**
   * @brief Remove key and child at index
   */
  void remove_at(uint32_t index) {
    uint32_t n = num_keys();
    if (index >= n)
      return;

    // Shift keys and children
    for (uint32_t i = index; i < n - 1; ++i) {
      set_key_at(i, key_at(i + 1));
      set_child_at(i + 1, child_at(i + 2));
    }

    decrement_num_keys();
  }

private:
  // Layout: child_0 | key_0 | child_1 | key_1 | ...
  // First child is at offset 0
  // Key i is at offset: sizeof(page_id_t) + i * (sizeof(key) +
  // sizeof(page_id_t)) Child i is at offset: i * (sizeof(key) +
  // sizeof(page_id_t)) for i > 0
  //                       0 for i = 0

  [[nodiscard]] const char *child_ptr(uint32_t index) const {
    if (index == 0) {
      return data_area();
    }
    return data_area() + sizeof(page_id_t) + (index - 1) * kPairSize +
           sizeof(BPTreeKey);
  }

  [[nodiscard]] char *child_ptr_mut(uint32_t index) {
    return const_cast<char *>(child_ptr(index));
  }

  [[nodiscard]] const char *key_ptr(uint32_t index) const {
    return data_area() + sizeof(page_id_t) + index * kPairSize;
  }

  [[nodiscard]] char *key_ptr_mut(uint32_t index) {
    return const_cast<char *>(key_ptr(index));
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// Leaf Node Page
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Leaf node in B+ tree
 *
 * Stores keys and values (RIDs).
 * Linked to sibling leaves for range scans.
 *
 * Layout: next_leaf_id | prev_leaf_id | key_0 | value_0 | key_1 | value_1 | ...
 */
class BPTreeLeafPage : public BPTreePage {
public:
  /// Size of sibling pointers
  static constexpr size_t kSiblingSize = sizeof(page_id_t) * 2;

  /// Size of each key-value pair
  static constexpr size_t kPairSize = sizeof(BPTreeKey) + sizeof(BPTreeValue);

  explicit BPTreeLeafPage(Page *page) : BPTreePage(page) {}

  /**
   * @brief Initialize as leaf node
   */
  void init(uint32_t max_keys) {
    set_page_type(BPTreePageType::LEAF);
    set_num_keys(0);
    set_max_size(max_keys);
    set_parent_page_id(INVALID_PAGE_ID);
    set_next_leaf_id(INVALID_PAGE_ID);
    set_prev_leaf_id(INVALID_PAGE_ID);
  }

  /**
   * @brief Calculate max keys for given page size
   */
  static uint32_t compute_max_size() {
    // Available space = page_size - page_header - btree_header - sibling_ptrs
    size_t available = config::kDefaultPageSize - config::kPageHeaderSize -
                       kHeaderSize - kSiblingSize;
    return static_cast<uint32_t>(available / kPairSize);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Sibling Access
  // ─────────────────────────────────────────────────────────────────────────

  [[nodiscard]] page_id_t next_leaf_id() const {
    page_id_t id;
    std::memcpy(&id, data_area(), sizeof(id));
    return id;
  }

  void set_next_leaf_id(page_id_t id) {
    std::memcpy(data_area(), &id, sizeof(id));
    page_->set_dirty(true);
  }

  [[nodiscard]] page_id_t prev_leaf_id() const {
    page_id_t id;
    std::memcpy(&id, data_area() + sizeof(page_id_t), sizeof(id));
    return id;
  }

  void set_prev_leaf_id(page_id_t id) {
    std::memcpy(data_area() + sizeof(page_id_t), &id, sizeof(id));
    page_->set_dirty(true);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Key-Value Access
  // ─────────────────────────────────────────────────────────────────────────

  [[nodiscard]] BPTreeKey key_at(uint32_t index) const {
    if (index >= num_keys())
      return 0;
    BPTreeKey key;
    std::memcpy(&key, key_ptr(index), sizeof(key));
    return key;
  }

  void set_key_at(uint32_t index, BPTreeKey key) {
    if (index < max_size()) {
      std::memcpy(key_ptr_mut(index), &key, sizeof(key));
      page_->set_dirty(true);
    }
  }

  [[nodiscard]] BPTreeValue value_at(uint32_t index) const {
    if (index >= num_keys())
      return BPTreeValue();
    BPTreeValue value;
    std::memcpy(&value, value_ptr(index), sizeof(value));
    return value;
  }

  void set_value_at(uint32_t index, const BPTreeValue &value) {
    if (index < max_size()) {
      std::memcpy(value_ptr_mut(index), &value, sizeof(value));
      page_->set_dirty(true);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Search
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Find the index of a key (or where it should be inserted)
   *
   * @param key The key to search for
   * @param found[out] Set to true if key was found
   * @return Index of key or insertion point
   */
  [[nodiscard]] uint32_t find_key_index(BPTreeKey key,
                                        bool *found = nullptr) const {
    uint32_t n = num_keys();
    if (found)
      *found = false;

    if (n == 0)
      return 0;

    // Binary search
    uint32_t left = 0;
    uint32_t right = n;

    while (left < right) {
      uint32_t mid = left + (right - left) / 2;
      BPTreeKey mid_key = key_at(mid);

      if (mid_key == key) {
        if (found)
          *found = true;
        return mid;
      } else if (mid_key < key) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    return left;
  }

  /**
   * @brief Find value for a key
   */
  [[nodiscard]] std::optional<BPTreeValue> find(BPTreeKey key) const {
    bool found;
    uint32_t idx = find_key_index(key, &found);
    if (found) {
      return value_at(idx);
    }
    return std::nullopt;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Insertion
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Insert a key-value pair
   *
   * @return true if successful, false if full or duplicate
   */
  bool insert(BPTreeKey key, const BPTreeValue &value) {
    if (is_full())
      return false;

    bool found;
    uint32_t idx = find_key_index(key, &found);

    // Don't allow duplicates
    if (found)
      return false;

    // Shift to make room
    uint32_t n = num_keys();
    for (uint32_t i = n; i > idx; --i) {
      set_key_at(i, key_at(i - 1));
      set_value_at(i, value_at(i - 1));
    }

    // Insert
    set_key_at(idx, key);
    set_value_at(idx, value);
    increment_num_keys();

    return true;
  }

  /**
   * @brief Remove a key
   *
   * @return true if key was found and removed
   */
  bool remove(BPTreeKey key) {
    bool found;
    uint32_t idx = find_key_index(key, &found);

    if (!found)
      return false;

    // Shift to fill gap
    uint32_t n = num_keys();
    for (uint32_t i = idx; i < n - 1; ++i) {
      set_key_at(i, key_at(i + 1));
      set_value_at(i, value_at(i + 1));
    }

    decrement_num_keys();
    return true;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Splitting
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Move half of entries to a new sibling page
   *
   * @param sibling The new sibling page (should be initialized but empty)
   * @return The first key in the sibling (for parent)
   */
  BPTreeKey move_half_to(BPTreeLeafPage *sibling) {
    uint32_t n = num_keys();
    uint32_t mid = n / 2;

    // Copy second half to sibling
    for (uint32_t i = mid; i < n; ++i) {
      sibling->set_key_at(i - mid, key_at(i));
      sibling->set_value_at(i - mid, value_at(i));
    }
    sibling->set_num_keys(n - mid);

    // Update our count
    set_num_keys(mid);

    // Return the first key of sibling (for parent)
    return sibling->key_at(0);
  }

  /**
   * @brief Merge all entries from right sibling into this leaf
   *
   * Used during delete when combined entries fit in one node.
   * The right sibling should be deleted after this call.
   */
  void merge_from_right(BPTreeLeafPage *right_sibling) {
    uint32_t my_keys = num_keys();
    uint32_t sibling_keys = right_sibling->num_keys();

    // Copy all entries from sibling
    for (uint32_t i = 0; i < sibling_keys; ++i) {
      set_key_at(my_keys + i, right_sibling->key_at(i));
      set_value_at(my_keys + i, right_sibling->value_at(i));
    }
    set_num_keys(my_keys + sibling_keys);

    // Update sibling pointers
    set_next_leaf_id(right_sibling->next_leaf_id());
  }

  /**
   * @brief Borrow first entry from right sibling
   *
   * @return The new key that should replace the parent key
   */
  BPTreeKey borrow_from_right(BPTreeLeafPage *right_sibling) {
    uint32_t my_keys = num_keys();

    // Take first entry from sibling
    set_key_at(my_keys, right_sibling->key_at(0));
    set_value_at(my_keys, right_sibling->value_at(0));
    increment_num_keys();

    // Shift sibling entries left
    uint32_t sibling_keys = right_sibling->num_keys();
    for (uint32_t i = 0; i < sibling_keys - 1; ++i) {
      right_sibling->set_key_at(i, right_sibling->key_at(i + 1));
      right_sibling->set_value_at(i, right_sibling->value_at(i + 1));
    }
    right_sibling->decrement_num_keys();

    // Return the new first key of sibling (for parent update)
    return right_sibling->key_at(0);
  }

  /**
   * @brief Borrow last entry from left sibling
   *
   * @return The key that should replace the parent key (our new first key)
   */
  BPTreeKey borrow_from_left(BPTreeLeafPage *left_sibling) {
    uint32_t my_keys = num_keys();
    uint32_t sibling_keys = left_sibling->num_keys();

    // Shift our entries right to make room
    for (uint32_t i = my_keys; i > 0; --i) {
      set_key_at(i, key_at(i - 1));
      set_value_at(i, value_at(i - 1));
    }

    // Take last entry from sibling
    set_key_at(0, left_sibling->key_at(sibling_keys - 1));
    set_value_at(0, left_sibling->value_at(sibling_keys - 1));
    increment_num_keys();

    left_sibling->decrement_num_keys();

    // Return our new first key (for parent update)
    return key_at(0);
  }

private:
  // Layout after header: next_leaf_id | prev_leaf_id | key_0 | value_0 | ...

  [[nodiscard]] const char *key_ptr(uint32_t index) const {
    return data_area() + kSiblingSize + index * kPairSize;
  }

  [[nodiscard]] char *key_ptr_mut(uint32_t index) {
    return const_cast<char *>(key_ptr(index));
  }

  [[nodiscard]] const char *value_ptr(uint32_t index) const {
    return data_area() + kSiblingSize + index * kPairSize + sizeof(BPTreeKey);
  }

  [[nodiscard]] char *value_ptr_mut(uint32_t index) {
    return const_cast<char *>(value_ptr(index));
  }
};

} // namespace entropy
