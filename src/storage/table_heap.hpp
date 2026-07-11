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

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_set>
#include <vector>

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
   * @param before_publish Optional hook invoked with the new RID after the
   *        record is placed but before the insert is published (the heap's
   *        exclusive lock is still held, so iterators — which take the shared
   *        lock — cannot yet observe the slot). Used to register MVCC
   *        metadata atomically with the insert so no reader can see the bytes
   *        before their version exists. If the hook fails, the record is
   *        removed and the hook's error returned: the insert never becomes
   *        visible.
   * @param checkpoint_barrier Optional writer-quiesce latch
   *        (TransactionManager::checkpoint_barrier()). Held SHARED across the
   *        heap mutation + the publication hook (which appends the WAL record
   *        and stamps the page LSN), so a concurrent checkpoint — which takes
   *        it EXCLUSIVELY around its page flush — can never capture/flush the
   *        mutated page before its log record is stamped (crash-safety F3).
   *        Acquired AFTER the heap's own lock, preserving heap-lock → barrier.
   * @param slot_reserved Optional predicate that keeps the free-slot search off
   *        slots reserved by an uncommitted DELETE (see SlotReservedFn).
   * @return Status::Ok() on success
   *
   * The hook runs under the heap's exclusive lock: it must not re-enter this
   * TableHeap and must not block on other transactions (e.g. lock waits).
   */
  [[nodiscard]] Status
  insert_tuple(const Tuple &tuple, RID *rid,
               const std::function<Status(RID)> &before_publish = nullptr,
               std::shared_mutex *checkpoint_barrier = nullptr,
               const SlotReservedFn &slot_reserved = nullptr);

  /**
   * @brief Delete a tuple from the table
   * @param rid Record ID of tuple to delete
   * @param on_logged Optional hook run after the slot is freed while the heap
   *        lock (and the checkpoint barrier, if given) is still held. Used to
   *        append the DELETE's WAL record and stamp the page LSN inside the
   *        same critical section as the mutation (crash-safety F3).
   * @param checkpoint_barrier Optional writer-quiesce latch held SHARED across
   *        the free + @p on_logged (see insert_tuple).
   * @return Status::Ok() on success, Status::NotFound() if RID invalid
   */
  [[nodiscard]] Status
  delete_tuple(const RID &rid, const std::function<void()> &on_logged = nullptr,
               std::shared_mutex *checkpoint_barrier = nullptr);

  /**
   * @brief Restore a previously deleted tuple at its original RID
   *
   * Used by transaction abort to undo DELETE. The slot at rid must be empty.
   *
   * @param rid Original record ID to restore into
   * @param tuple Tuple data to restore
   * @return Status::Ok() on success
   */
  [[nodiscard]] Status restore_tuple(const RID &rid, const Tuple &tuple);

  /**
   * @brief Update a tuple in the table
   * @param tuple New tuple data
   * @param rid Record ID of tuple to update
   * @param new_rid Optional output: if non-null and RID changed, receives new
   * RID
   * @return Status::Ok() on success, Status::NotFound() if RID invalid
   *
   * If the new tuple is too large for in-place update, this will delete
   * the old tuple and insert the new one at a different location.
   * If new_rid is provided, it will be set to the new location.
   * If new_rid is null and the tuple must be moved, this still succeeds
   * but the caller won't know the new location.
   */
  [[nodiscard]] Status update_tuple(const Tuple &tuple, const RID &rid,
                                    RID *new_rid = nullptr);

  /**
   * @brief Update a tuple only if the new bytes fit at its current location
   *
   * Unlike update_tuple, never relocates: when the new tuple does not fit in
   * place this returns Status::OutOfMemory with NO side effects, letting a
   * transactional caller drive the relocation itself (as a logged
   * delete+insert with version metadata). Returns Status::NotFound when no
   * record exists at @p rid.
   *
   * @param on_logged Optional hook run after the in-place mutation while the
   *        heap lock (and barrier) is still held — appends the UPDATE's WAL
   *        record and stamps the page LSN in the same critical section as the
   *        mutation (crash-safety F3). Not run on the no-side-effect failure
   *        paths.
   * @param checkpoint_barrier Optional writer-quiesce latch held SHARED across
   *        the mutation + @p on_logged (see insert_tuple).
   */
  [[nodiscard]] Status
  update_tuple_in_place(const Tuple &tuple, const RID &rid,
                        const std::function<void()> &on_logged = nullptr,
                        std::shared_mutex *checkpoint_barrier = nullptr);

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
   * @param include_empty_slots When true, the iterator also yields empty
   *        slots as empty tuples carrying a valid RID. MVCC scans use this to
   *        consult the version store for "ghost" rows — RIDs whose heap slot
   *        was freed by an in-flight or later-committed DELETE but whose
   *        retained before-image is still visible to an older snapshot.
   */
  [[nodiscard]] TableIterator begin(bool include_empty_slots = false);

  /**
   * @brief Get an iterator to the end of the table
   */
  [[nodiscard]] TableIterator end();

  // ─────────────────────────────────────────────────────────────────────────
  // Lifecycle
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * @brief Ensure the heap has its first page allocated
   *
   * Idempotent. Allocating the first page eagerly (at table creation) makes
   * the heap's first_page_id deterministic and recordable in the catalog
   * manifest, so the heap can be rebuilt after a restart.
   *
   * @return Status::Ok() on success (including when a first page already
   *         exists), an error otherwise.
   */
  [[nodiscard]] Status ensure_first_page();

  /**
   * @brief Deallocate every page owned by the heap
   *
   * Walks the page chain and reclaims each page through the buffer pool
   * (which forwards to DiskManager::deallocate_page). Used by drop_table so
   * a dropped table's heap pages are not orphaned. After this call the heap
   * is empty (first_page_id() == INVALID_PAGE_ID).
   *
   * @param deallocate_disk When false, the pages are only DISCARDED from the
   *        buffer pool (their dirty bytes dropped, not flushed) while their ids
   *        stay allocated; the caller frees the ids later, once it is crash-safe
   *        to reuse them. DROP TABLE defers this past its checkpoint (F2).
   */
  [[nodiscard]] Status reclaim_all_pages(bool deallocate_disk = true);

  /**
   * @brief Collect the ids of every page currently owned by the heap
   *
   * Used when dropping a table to purge version-store chains keyed by RIDs on
   * these pages before the pages are reclaimed for reuse (hence a set: its
   * sole consumer does membership tests).
   */
  [[nodiscard]] std::unordered_set<page_id_t> page_ids() const;

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
  // The iterator takes the heap's shared lock while reading slots so an
  // in-flight insert (bytes + MVCC registration under the exclusive lock) is
  // observed atomically or not at all.
  friend class TableIterator;

  /**
   * @brief Create a new page and append it to the heap
   * @return Page ID of the new page, or INVALID_PAGE_ID on failure
   */
  page_id_t create_new_page();

  /**
   * @brief Find a page with enough space for a tuple
   * @param size Required space in bytes
   * @param slot_reserved Predicate forwarded to can_fit so a page whose only
   *        free slot is reserved is not treated as having reusable space.
   * @return Page ID, or INVALID_PAGE_ID if none found
   */
  page_id_t find_page_with_space(uint32_t size,
                                 const SlotReservedFn &slot_reserved);

  std::shared_ptr<BufferPoolManager> buffer_pool_;
  page_id_t first_page_id_ = INVALID_PAGE_ID;
  page_id_t last_page_id_ = INVALID_PAGE_ID;
  mutable std::shared_mutex mutex_; /// Protects table heap operations
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
   * @param include_empty_slots See TableHeap::begin
   */
  TableIterator(TableHeap *table_heap, RID rid,
                bool include_empty_slots = false);

  TableIterator(const TableIterator &other);
  TableIterator &operator=(const TableIterator &other);
  TableIterator(TableIterator &&other) noexcept;
  TableIterator &operator=(TableIterator &&other) noexcept;
  ~TableIterator();

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
  void release_page();

  TableHeap *table_heap_ = nullptr;
  RID rid_;
  bool include_empty_slots_ = false;
  Tuple current_tuple_;
  std::shared_ptr<BufferPoolManager> buffer_pool_;
  Page *pinned_page_ = nullptr;
  page_id_t pinned_page_id_ = INVALID_PAGE_ID;
  bool owns_pin_ = false;
};

} // namespace entropy
