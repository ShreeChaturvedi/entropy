#pragma once

/**
 * @file b_plus_tree.hpp
 * @brief B+ Tree index implementation
 *
 * A B+ Tree is a self-balancing tree data structure that maintains sorted data
 * and allows searches, sequential access, insertions, and deletions in O(log
 * n).
 *
 * Properties:
 * - All values are stored in leaf nodes
 * - Internal nodes only store keys for navigation
 * - Leaf nodes are linked for efficient range scans
 * - Tree is always balanced (all leaves at same depth)
 */

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "common/types.hpp"
#include "entropy/status.hpp"
#include "storage/b_plus_tree_page.hpp"
#include "storage/buffer_pool.hpp"

namespace entropy {

// Forward declarations
class BPlusTreeIterator;
/// Per-operation set of write-latched + pinned pages; defined in b_plus_tree.cpp.
class WriteSet;

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

  /**
   * @brief Construct a B+ Tree with explicit node capacities
   *
   * Used by tests to force splits/merges with a small fanout.
   */
  BPlusTree(std::shared_ptr<BufferPoolManager> buffer_pool,
            page_id_t root_page_id, uint32_t leaf_max_size,
            uint32_t internal_max_size);

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
  [[nodiscard]] Status insert(KeyType key, const ValueType &value);

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
  [[nodiscard]] std::vector<std::pair<KeyType, ValueType>>
  range_scan(KeyType start_key, KeyType end_key);

  /**
   * @brief Deallocate every page owned by the tree
   *
   * Traverses the tree and frees each page through the buffer pool (which
   * forwards to DiskManager::deallocate_page). Used when the owning index is
   * dropped. Deliberately does NOT fire the root-change callback: the caller
   * is destroying the tree and records the removal itself.
   */
  [[nodiscard]] Status reclaim_all_pages();

  /**
   * @brief Write every page owned by the tree to disk
   *
   * flush_page works on pinned pages, so this is safe to call from the
   * root-change callback while the mutation that moved the root still holds
   * pins. Fails on the first page the buffer pool cannot write.
   */
  [[nodiscard]] Status flush_all_pages();

  /**
   * @brief Register a callback invoked whenever the root page id changes
   *
   * The root moves on first insert, root split, and root collapse. The
   * catalog uses this to keep the durable manifest's root_page_id current.
   * Register before the tree is shared across threads; the callback runs on
   * the thread performing the mutation.
   */
  void set_root_change_callback(std::function<void(page_id_t)> callback) {
    root_change_callback_ = std::move(callback);
  }

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
   *
   * Acquire load, paired with change_root's release store: a reader that
   * observes a new root id also observes every write the publishing thread
   * made to that root page before publishing it (the grown root is installed
   * without holding its page latch, so this pairing is what orders its bytes).
   */
  [[nodiscard]] page_id_t root_page_id() const noexcept {
    return root_page_id_.load(std::memory_order_acquire);
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
   * @brief Pin and read-latch the current root page.
   *
   * A writer can only move the root while holding the old root's write latch,
   * so after latching we re-check root_page_id_ and retry if it moved between
   * the snapshot and the latch. Returns the root pinned and read-latched (the
   * caller must runlatch() then unpin_page()), or nullptr on an empty tree or
   * fetch failure.
   */
  [[nodiscard]] Page *latch_root_read() const;

  /**
   * @brief Descend to the leaf that should contain @p key, read-latch-crabbing.
   *
   * Starts from latch_root_read(), then hand-over-hand takes each child's read
   * latch and releases the parent. Returns the leaf pinned and holding its READ
   * latch; the caller must runlatch() then unpin_page(). Returns nullptr on an
   * empty tree or fetch failure.
   */
  [[nodiscard]] Page *descend_to_leaf_read(KeyType key);

  /**
   * @brief Create a new leaf page
   * @return The new page, or nullptr on failure
   */
  [[nodiscard]] Page *create_leaf_page(page_id_t *page_id);

  /**
   * @brief Create a new internal page
   * @return The new page, or nullptr on failure
   */
  [[nodiscard]] Page *create_internal_page(page_id_t *page_id);

  // ─────────────────────────────────────────────────────────────────────────
  // Structural-write helpers (latch-crabbing)
  //
  // These run while write_mutex_ is held and operate on the retained ancestor
  // chain `path` (root-first, leaf-last) whose pages are write-latched in `ws`.
  // Siblings and new pages are latched through `ws` on demand.
  // ─────────────────────────────────────────────────────────────────────────

  /// Propagate a split (@p up_key separating the node at path.back() from its
  /// new right sibling @p up_child) up the retained @p path, splitting internal
  /// nodes and growing a new root as needed.
  [[nodiscard]] Status insert_into_parents(WriteSet &ws,
                                           std::vector<page_id_t> &path,
                                           KeyType up_key, page_id_t up_child);

  /// Point every child of @p internal at @p parent_id (fixes parent pointers
  /// after a split/merge moves children between nodes).
  void reparent_children(page_id_t parent_id, BPTreeInternalPage &internal);

  /// Set @p child_id's parent pointer to @p parent_id (no-op for INVALID).
  ///
  /// Deliberately latch-free (pin, write the field, unpin): parent_page_id is
  /// written and read ONLY by structural writers, which write_mutex_
  /// serializes, and readers never access the field. Latching here would have
  /// to write-latch children LEFT of leaf latches the operation already holds
  /// (e.g. a merged node's child 0), inverting the global left-to-right order
  /// against leaf-chain scans -- the deadlock found in review.
  void set_parent(page_id_t child_id, page_id_t parent_id);

  /// Rebalance an underfull leaf (path.back()) by borrowing from or merging with
  /// a sibling, then propagate any resulting parent underflow.
  [[nodiscard]] Status handle_leaf_underflow(WriteSet &ws,
                                             std::vector<page_id_t> &path);

  /// After a child merge removed a key from the node at path.back(), collapse
  /// the root if it emptied, or rebalance the node if it underflowed.
  [[nodiscard]] Status handle_parent_after_merge(WriteSet &ws,
                                                 std::vector<page_id_t> &path);

  /// Rebalance an underfull internal node (path.back()) by borrowing from or
  /// merging with a sibling, then recurse upward.
  [[nodiscard]] Status handle_internal_underflow(WriteSet &ws,
                                                 std::vector<page_id_t> &path);

  /// Release @p id from @p ws and delete it from the pool; if the pool refuses
  /// (a reader in its unlatch-then-unpin window still holds a pin), queue the
  /// id so a later structural write retries instead of leaking it on disk.
  void free_page_or_defer(WriteSet &ws, page_id_t id);

  /// Retry deallocation of pages refused earlier. Caller holds write_mutex_.
  void drain_deferred_free();

  /**
   * @brief Assign a new root page id and notify the root-change callback
   */
  void change_root(page_id_t new_root_id);

  /**
   * @brief Collect the ids of every intact page reachable from the root
   *
   * Cycle- and corruption-tolerant: uses a visited set and skips pages that
   * are not valid index nodes.
   */
  [[nodiscard]] std::vector<page_id_t> collect_tree_pages();

  std::shared_ptr<BufferPoolManager> buffer_pool_;
  /// Atomic so a reader can snapshot the root while a writer moves it.
  std::atomic<page_id_t> root_page_id_;
  std::function<void(page_id_t)> root_change_callback_;

  /// Serializes structural writers (insert / remove / reclaim). Readers do NOT
  /// take this; they synchronize with writers through per-page latches acquired
  /// via crabbing. Serialization removes writer-vs-writer latch interactions;
  /// reader-vs-writer deadlock freedom comes from the single global latch order
  /// (ancestors before descendants, left before right) that every code path
  /// honors. See the design notes in b_plus_tree.cpp.
  std::mutex write_mutex_;

  /// Page ids whose delete_page was refused by a transient reader pin.
  /// Guarded by write_mutex_ (only structural writers touch it).
  std::vector<page_id_t> deferred_free_;

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
  BPlusTreeIterator(BPlusTree *tree, page_id_t leaf_page_id, uint32_t index);

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
  BPlusTreeIterator &operator++();

  /**
   * @brief Post-increment
   */
  BPlusTreeIterator operator++(int);

  /**
   * @brief Check if at end
   */
  [[nodiscard]] bool is_end() const { return leaf_page_id_ == INVALID_PAGE_ID; }

  /**
   * @brief Equality comparison
   */
  bool operator==(const BPlusTreeIterator &other) const {
    return leaf_page_id_ == other.leaf_page_id_ && index_ == other.index_;
  }

  bool operator!=(const BPlusTreeIterator &other) const {
    return !(*this == other);
  }

private:
  BPlusTree *tree_ = nullptr;
  page_id_t leaf_page_id_ = INVALID_PAGE_ID;
  uint32_t index_ = 0;
};

} // namespace entropy
