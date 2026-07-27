#pragma once

/**
 * @file table_store.hpp
 * @brief Abstract table storage interface
 *
 * TableStore is the catalog/executor-facing surface for heap-organized tuple
 * storage. TableHeap is the sole concrete implementation today. Crash-safety
 * hook signatures (before_publish, on_logged, checkpoint_barrier, slot_reserved)
 * match TableHeap exactly so callers need no dual code paths.
 */

#include <functional>
#include <memory>
#include <shared_mutex>
#include <unordered_set>
#include <vector>

#include "common/types.hpp"
#include "entropy/status.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/table_page.hpp" // SlotReservedFn
#include "storage/tuple.hpp"

namespace entropy {

/**
 * @brief Abstract table storage (heap file)
 *
 * Iteration (begin/end) stays on TableHeap: TableIterator is concrete and
 * friends TableHeap for the shared heap mutex. Call sites that scan keep a
 * TableHeap* / shared_ptr<TableHeap>; CRUD call sites may use TableStore*.
 */
class TableStore {
public:
  virtual ~TableStore() = default;

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
   * store and must not block on other transactions (e.g. lock waits).
   */
  [[nodiscard]] virtual Status
  insert_tuple(const Tuple &tuple, RID *rid,
               const std::function<Status(RID)> &before_publish = nullptr,
               std::shared_mutex *checkpoint_barrier = nullptr,
               const SlotReservedFn &slot_reserved = nullptr) = 0;

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
  [[nodiscard]] virtual Status
  delete_tuple(const RID &rid, const std::function<void()> &on_logged = nullptr,
               std::shared_mutex *checkpoint_barrier = nullptr) = 0;

  /**
   * @brief Restore a previously deleted tuple at its original RID
   *
   * Used by transaction abort to undo DELETE. The slot at rid must be empty.
   */
  [[nodiscard]] virtual Status restore_tuple(const RID &rid,
                                             const Tuple &tuple) = 0;

  /**
   * @brief Update a tuple in the table
   *
   * If the new tuple is too large for in-place update, this will delete the
   * old tuple and insert the new one at a different location.
   */
  [[nodiscard]] virtual Status update_tuple(const Tuple &tuple, const RID &rid,
                                            RID *new_rid = nullptr) = 0;

  /**
   * @brief Update a tuple only if the new bytes fit at its current location
   *
   * Unlike update_tuple, never relocates: when the new tuple does not fit in
   * place this returns Status::OutOfMemory with NO side effects.
   */
  [[nodiscard]] virtual Status update_tuple_in_place(
      const Tuple &tuple, const RID &rid,
      const std::function<void()> &on_logged = nullptr,
      std::shared_mutex *checkpoint_barrier = nullptr) = 0;

  /**
   * @brief Get a tuple by RID
   */
  [[nodiscard]] virtual Status get_tuple(const RID &rid, Tuple *tuple) = 0;

  /**
   * @brief Ensure the heap has its first page allocated (idempotent)
   */
  [[nodiscard]] virtual Status ensure_first_page() = 0;

  /**
   * @brief Deallocate or discard every page owned by the store
   *
   * @param deallocate_disk When false, pages are only discarded from the buffer
   *        pool while their ids stay allocated (DROP TABLE deferral, F2).
   */
  [[nodiscard]] virtual Status
  reclaim_all_pages(bool deallocate_disk = true) = 0;

  /**
   * @brief Collect ids of every page currently owned by the store
   */
  [[nodiscard]] virtual std::unordered_set<page_id_t> page_ids() const = 0;

  /**
   * @brief First page ID in the heap chain
   */
  [[nodiscard]] virtual page_id_t first_page_id() const noexcept = 0;

  /**
   * @brief Whether the table has no pages
   */
  [[nodiscard]] virtual bool is_empty() const noexcept = 0;

  /**
   * @brief Buffer pool backing this store
   */
  [[nodiscard]] virtual std::shared_ptr<BufferPoolManager>
  buffer_pool() const = 0;
};

} // namespace entropy
