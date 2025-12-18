#pragma once

/**
 * @file table_heap.hpp
 * @brief Table storage using heap file organization
 *
 * A heap file is an unordered collection of pages where tuples are stored.
 * Pages are linked together in a doubly-linked list.
 *
 * Structure:
 * - first_page_id_ points to the first page in the heap
 * - Each page maintains next/prev links
 * - New tuples are inserted into pages with available space
 * - Deleted tuples leave "holes" that can be reused
 *
 * Thread Safety:
 * - Uses shared_mutex for concurrent read access
 * - Write operations (insert/update/delete) hold exclusive lock
 */

#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>

#include "common/types.hpp"
#include "entropy/status.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/table_page.hpp"
#include "storage/tuple.hpp"

namespace entropy {

// Forward declaration
class TableIterator;

/**
 * @brief Table Heap - stores tuples in a heap file
 *
 * Provides CRUD operations on tuples stored in a heap file.
 * The heap file is organized as a linked list of pages.
 */
class TableHeap {
public:
  /**
   * @brief Construct a new table heap
   * @param buffer_pool Buffer pool manager for page access
   */
  explicit TableHeap(std::shared_ptr<BufferPoolManager> buffer_pool);

  /**
   * @brief Construct a table heap with existing first page
   * @param buffer_pool Buffer pool manager for page access
   * @param first_page_id ID of the first page in the heap
   */
  TableHeap(std::shared_ptr<BufferPoolManager> buffer_pool,
            page_id_t first_page_id);

  ~TableHeap() = default;

  // ─────────────────────────────────────────────────────────────────────────
  // Tuple Operations
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Insert a tuple into the table
   * @param tuple Tuple to insert
   * @param[out] rid Record ID where tuple was inserted
   * @return Status::Ok() on success
   */
  [[nodiscard]] Status insert_tuple(const Tuple &tuple, RID *rid);

  /**
   * @brief Delete a tuple from the table
   * @param rid Record ID of tuple to delete
   * @return Status::Ok() on success, Status::NotFound() if RID invalid
   */
  [[nodiscard]] Status delete_tuple(const RID &rid);

  /**
   * @brief Update a tuple in the table
   * @param tuple New tuple data
   * @param rid Record ID of tuple to update
   * @return Status::Ok() on success, Status::NotFound() if RID invalid
   *
   * Note: If the new tuple is too large for in-place update,
   * this will delete the old tuple and insert the new one,
   * potentially returning a different RID.
   */
  [[nodiscard]] Status update_tuple(const Tuple &tuple, const RID &rid);

  /**
   * @brief Get a tuple by RID
   * @param rid Record ID of tuple to retrieve
   * @param[out] tuple Retrieved tuple
   * @return Status::Ok() on success, Status::NotFound() if RID invalid
   */
  [[nodiscard]] Status get_tuple(const RID &rid, Tuple *tuple);

  // ─────────────────────────────────────────────────────────────────────────
  // Iteration
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Get an iterator to the beginning of the table
   */
  [[nodiscard]] TableIterator begin();

  /**
   * @brief Get an iterator to the end of the table
   */
  [[nodiscard]] TableIterator end();

  // ─────────────────────────────────────────────────────────────────────────
  // Accessors
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Get the first page ID in the heap
   */
  [[nodiscard]] page_id_t first_page_id() const noexcept {
    return first_page_id_;
  }

  /**
   * @brief Check if the table is empty
   */
  [[nodiscard]] bool is_empty() const noexcept {
    return first_page_id_ == INVALID_PAGE_ID;
  }

  /**
   * @brief Get the buffer pool manager
   */
  [[nodiscard]] std::shared_ptr<BufferPoolManager> buffer_pool() const {
    return buffer_pool_;
  }

private:
  /**
   * @brief Create a new page and append it to the heap
   * @return Page ID of the new page, or INVALID_PAGE_ID on failure
   */
  page_id_t create_new_page();

  /**
   * @brief Find a page with enough space for a tuple
   * @param size Required space in bytes
   * @return Page ID, or INVALID_PAGE_ID if none found
   */
  page_id_t find_page_with_space(uint32_t size);

  std::shared_ptr<BufferPoolManager> buffer_pool_;
  page_id_t first_page_id_ = INVALID_PAGE_ID;
  page_id_t last_page_id_ = INVALID_PAGE_ID;
  mutable std::shared_mutex mutex_;  /// Protects table heap operations
};

/**
 * @brief Iterator for scanning a table heap
 *
 * Iterates through all valid (non-deleted) tuples in the table.
 */
class TableIterator {
public:
  /**
   * @brief Construct an end iterator
   */
  TableIterator() = default;

  /**
   * @brief Construct an iterator starting at a specific position
   */
  TableIterator(TableHeap *table_heap, RID rid);

  /**
   * @brief Dereference operator - get current tuple
   */
  [[nodiscard]] const Tuple &operator*() const { return current_tuple_; }

  /**
   * @brief Arrow operator - access tuple members
   */
  [[nodiscard]] const Tuple *operator->() const { return &current_tuple_; }

  /**
   * @brief Pre-increment - move to next tuple
   */
  TableIterator &operator++();

  /**
   * @brief Post-increment - move to next tuple
   */
  TableIterator operator++(int);

  /**
   * @brief Equality comparison
   */
  bool operator==(const TableIterator &other) const {
    return rid_ == other.rid_;
  }

  /**
   * @brief Inequality comparison
   */
  bool operator!=(const TableIterator &other) const {
    return !(*this == other);
  }

  /**
   * @brief Get the current RID
   */
  [[nodiscard]] RID rid() const noexcept { return rid_; }

  /**
   * @brief Check if iterator is valid
   */
  [[nodiscard]] bool is_valid() const noexcept { return rid_.is_valid(); }

private:
  /**
   * @brief Advance to the next valid tuple
   */
  void advance_to_next_valid();

  TableHeap *table_heap_ = nullptr;
  RID rid_;
  Tuple current_tuple_;
};

} // namespace entropy
