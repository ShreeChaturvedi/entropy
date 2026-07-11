/**
 * @file table_heap.cpp
 * @brief Table Heap implementation
 */

#include "storage/table_heap.hpp"

#include <cstring>
#include <unordered_set>
#include <utility>

#include "common/config.hpp"
#include "common/status.hpp"

namespace entropy {

namespace {

// Acquire the checkpoint barrier in SHARED mode when the caller supplied one.
// The returned lock must outlive the write's critical section; a null barrier
// yields an unlocked (no-op) guard for the test/no-transaction path.
std::shared_lock<std::shared_mutex>
acquire_checkpoint_barrier(std::shared_mutex *checkpoint_barrier) {
  if (checkpoint_barrier == nullptr) {
    return {};
  }
  return std::shared_lock<std::shared_mutex>(*checkpoint_barrier);
}

} // namespace

// ─────────────────────────────────────────────────────────────────────────────
// TableHeap Implementation
// ─────────────────────────────────────────────────────────────────────────────

TableHeap::TableHeap(std::shared_ptr<BufferPoolManager> buffer_pool)
    : buffer_pool_(std::move(buffer_pool)) {}

TableHeap::TableHeap(std::shared_ptr<BufferPoolManager> buffer_pool,
                     page_id_t first_page_id)
    : buffer_pool_(std::move(buffer_pool)), first_page_id_(first_page_id) {
  if (first_page_id_ == INVALID_PAGE_ID) {
    return;
  }

  // Find the last page by traversing the linked list. The chain may be
  // damaged if the process crashed after the catalog manifest was persisted
  // but before every referenced page reached disk: a never-written page reads
  // back zeroed (PageType::INVALID, next link 0), so a naive walk would loop
  // forever. Stop on non-table pages and on any cycle, and self-heal what we
  // can.
  std::unordered_set<page_id_t> visited;
  page_id_t prev_id = INVALID_PAGE_ID;
  page_id_t current_id = first_page_id_;
  while (current_id != INVALID_PAGE_ID) {
    Page *page = buffer_pool_->fetch_page(current_id);
    if (page == nullptr) {
      break;
    }
    TablePage table_page(page);

    if (page->page_type() != PageType::TABLE_PAGE) {
      if (current_id == first_page_id_) {
        // The eagerly allocated first page never reached disk. Re-initialize
        // it as an empty table page and flush it so the db file covers this
        // page id (otherwise a later allocate_page could hand it out again).
        table_page.init();
        buffer_pool_->unpin_page(current_id, true);
        buffer_pool_->flush_page(current_id);
        last_page_id_ = current_id;
      } else {
        // Truncate the chain at the last intact page.
        buffer_pool_->unpin_page(current_id, false);
        Page *prev_page = buffer_pool_->fetch_page(prev_id);
        if (prev_page != nullptr) {
          TablePage(prev_page).set_next_page_id(INVALID_PAGE_ID);
          buffer_pool_->unpin_page(prev_id, true);
        }
        last_page_id_ = prev_id;
      }
      return;
    }

    visited.insert(current_id);
    page_id_t next_id = table_page.get_next_page_id();
    if (next_id == current_id || visited.contains(next_id)) {
      next_id = INVALID_PAGE_ID; // Cycle — treat as end of chain.
    }
    if (next_id == INVALID_PAGE_ID) {
      last_page_id_ = current_id;
    }
    buffer_pool_->unpin_page(current_id, false);
    prev_id = current_id;
    current_id = next_id;
  }
}

Status TableHeap::ensure_first_page() {
  std::unique_lock lock(mutex_); // Exclusive lock for write

  if (first_page_id_ != INVALID_PAGE_ID) {
    return Status::Ok();
  }

  page_id_t page_id = create_new_page();
  if (page_id == INVALID_PAGE_ID) {
    return Status::OutOfMemory("Failed to allocate first heap page");
  }

  return Status::Ok();
}

Status TableHeap::reclaim_all_pages(bool deallocate_disk) {
  std::unique_lock lock(mutex_); // Exclusive lock for write

  std::unordered_set<page_id_t> visited;
  page_id_t current_id = first_page_id_;
  while (current_id != INVALID_PAGE_ID && !visited.contains(current_id)) {
    visited.insert(current_id);

    // Fetch to read the forward link before we free the page.
    Page *page = buffer_pool_->fetch_page(current_id);
    if (page == nullptr) {
      break;
    }
    // Never free a page that is not an intact table page: a damaged chain
    // (crash before the page reached disk) must not walk into foreign or
    // uninitialized pages.
    if (page->page_type() != PageType::TABLE_PAGE) {
      buffer_pool_->unpin_page(current_id, false);
      break;
    }
    page_id_t next_id = TablePage(page).get_next_page_id();
    buffer_pool_->unpin_page(current_id, false);
    // delete_page discards the buffered frame; with deallocate_disk it also
    // returns the id to DiskManager's free list for reuse. A deferred caller
    // (DROP TABLE) passes false and frees the ids after its checkpoint.
    buffer_pool_->delete_page(current_id, deallocate_disk);

    current_id = next_id;
  }

  first_page_id_ = INVALID_PAGE_ID;
  last_page_id_ = INVALID_PAGE_ID;

  return Status::Ok();
}

Status TableHeap::insert_tuple(const Tuple &tuple, RID *rid,
                               const std::function<Status(RID)> &before_publish,
                               std::shared_mutex *checkpoint_barrier,
                               const SlotReservedFn &slot_reserved) {
  std::unique_lock lock(mutex_); // Exclusive lock for write

  if (rid == nullptr) {
    return Status::InvalidArgument("RID output pointer is null");
  }

  if (tuple.is_empty()) {
    return Status::InvalidArgument("Cannot insert empty tuple");
  }

  uint32_t tuple_size = tuple.size();

  // Check if tuple is too large for a page
  // Account for slot overhead
  uint32_t max_tuple_size =
      static_cast<uint32_t>(config::kDefaultPageSize - config::kPageHeaderSize -
                            TablePage::kSlotSize);
  if (tuple_size > max_tuple_size) {
    return Status::InvalidArgument("Tuple too large for page");
  }

  // Find a page with enough space
  page_id_t target_page_id = find_page_with_space(tuple_size, slot_reserved);

  // If no page has space, create a new one
  if (target_page_id == INVALID_PAGE_ID) {
    target_page_id = create_new_page();
    if (target_page_id == INVALID_PAGE_ID) {
      return Status::OutOfMemory("Failed to allocate new page for tuple");
    }
  }

  // Fetch the target page and insert
  Page *page = buffer_pool_->fetch_page(target_page_id);
  if (page == nullptr) {
    return Status::IOError("Failed to fetch page for insert");
  }

  // Checkpoint barrier (shared): held across the heap mutation AND the
  // publication hook (which appends the WAL record + stamps the page LSN), so a
  // concurrent checkpoint cannot flush the mutated page ahead of its record.
  // Acquired here, after the heap lock, so the order stays heap-lock → barrier.
  auto barrier = acquire_checkpoint_barrier(checkpoint_barrier);

  TablePage table_page(page);
  auto slot_id = table_page.insert_record(
      tuple.data(), static_cast<uint16_t>(tuple_size), slot_reserved);

  if (!slot_id.has_value()) {
    // This shouldn't happen if find_page_with_space worked correctly
    buffer_pool_->unpin_page(target_page_id, false);
    return Status::Internal(
        "Failed to insert tuple into page with reported space");
  }

  // Set the RID
  rid->page_id = target_page_id;
  rid->slot_id = slot_id.value();

  // Publication barrier: run the hook while the exclusive lock is still held
  // and before the page is unpinned. Iterators take the shared lock to read
  // slots, so no reader can observe the new record until the hook (typically
  // MVCC version registration) has completed. On hook failure the record is
  // removed — the insert never becomes visible.
  if (before_publish) {
    Status hook_status = before_publish(*rid);
    if (!hook_status.ok()) {
      TablePage(page).delete_record(rid->slot_id);
      buffer_pool_->unpin_page(target_page_id, true);
      *rid = RID();
      return hook_status;
    }
  }

  buffer_pool_->unpin_page(target_page_id, true); // Mark as dirty

  return Status::Ok();
}

Status TableHeap::delete_tuple(const RID &rid,
                               const std::function<void()> &on_logged,
                               std::shared_mutex *checkpoint_barrier) {
  std::unique_lock lock(mutex_); // Exclusive lock for write

  if (!rid.is_valid()) {
    return Status::InvalidArgument("Invalid RID");
  }

  Page *page = buffer_pool_->fetch_page(rid.page_id);
  if (page == nullptr) {
    return Status::NotFound("Page not found");
  }

  // Barrier (shared) across the free + logging hook — see insert_tuple.
  auto barrier = acquire_checkpoint_barrier(checkpoint_barrier);

  TablePage table_page(page);
  bool deleted = table_page.delete_record(rid.slot_id);

  // Log while the mutation is still latched + barriered, so the record and its
  // page-LSN stamp land before the freed page can be checkpoint-flushed.
  if (deleted && on_logged) {
    on_logged();
  }

  buffer_pool_->unpin_page(rid.page_id, deleted); // Mark dirty if deleted

  if (!deleted) {
    return Status::NotFound("Tuple not found at specified RID");
  }

  return Status::Ok();
}

Status TableHeap::restore_tuple(const RID &rid, const Tuple &tuple) {
  std::unique_lock lock(mutex_);

  if (!rid.is_valid()) {
    return Status::InvalidArgument("Invalid RID");
  }
  if (tuple.is_empty()) {
    return Status::InvalidArgument("Cannot restore empty tuple");
  }

  Page *page = buffer_pool_->fetch_page(rid.page_id);
  if (page == nullptr) {
    return Status::NotFound("Page not found");
  }

  TablePage table_page(page);
  bool restored = table_page.insert_record_at(
      rid.slot_id, tuple.data(), static_cast<uint16_t>(tuple.size()));

  buffer_pool_->unpin_page(rid.page_id, restored);

  if (!restored) {
    return Status::Error("Failed to restore tuple at specified RID");
  }

  return Status::Ok();
}

Status TableHeap::update_tuple(const Tuple &tuple, const RID &rid,
                               RID *new_rid) {
  std::unique_lock lock(mutex_); // Exclusive lock for write

  if (!rid.is_valid()) {
    return Status::InvalidArgument("Invalid RID");
  }

  if (tuple.is_empty()) {
    return Status::InvalidArgument("Cannot update with empty tuple");
  }

  Page *page = buffer_pool_->fetch_page(rid.page_id);
  if (page == nullptr) {
    return Status::NotFound("Page not found");
  }

  TablePage table_page(page);

  // Try to update in place
  bool updated = table_page.update_record(rid.slot_id, tuple.data(),
                                          static_cast<uint16_t>(tuple.size()));

  if (updated) {
    buffer_pool_->unpin_page(rid.page_id, true);
    // RID didn't change for in-place update
    if (new_rid != nullptr) {
      *new_rid = rid;
    }
    return Status::Ok();
  }

  // In-place update failed (likely because new tuple is larger)
  // Check if the tuple exists at all
  auto old_record = table_page.get_record(rid.slot_id);
  if (old_record.empty()) {
    buffer_pool_->unpin_page(rid.page_id, false);
    return Status::NotFound("Tuple not found at specified RID");
  }

  // Delete the old record
  table_page.delete_record(rid.slot_id);
  buffer_pool_->unpin_page(rid.page_id, true);

  // Insert the new tuple (may go to a different page)
  // Release lock before recursive call to avoid deadlock
  lock.unlock();
  RID inserted_rid;
  Status status = insert_tuple(tuple, &inserted_rid);

  if (!status.ok()) {
    // This is a problem - we deleted the old tuple but couldn't insert the new
    // one. In a real system, we'd use WAL to handle this atomically.
    return Status::Internal("Failed to insert updated tuple: " +
                            std::string(status.message()));
  }

  // Return the new RID to the caller
  if (new_rid != nullptr) {
    *new_rid = inserted_rid;
  }

  return Status::Ok();
}

Status TableHeap::update_tuple_in_place(const Tuple &tuple, const RID &rid,
                                        const std::function<void()> &on_logged,
                                        std::shared_mutex *checkpoint_barrier) {
  std::unique_lock lock(mutex_); // Exclusive lock for write

  if (!rid.is_valid()) {
    return Status::InvalidArgument("Invalid RID");
  }
  if (tuple.is_empty()) {
    return Status::InvalidArgument("Cannot update with empty tuple");
  }

  Page *page = buffer_pool_->fetch_page(rid.page_id);
  if (page == nullptr) {
    return Status::NotFound("Page not found");
  }

  // Barrier (shared) across the in-place mutation + logging hook. Taken before
  // update_record probes/mutates so a checkpoint cannot flush a mutated page
  // ahead of its record — see insert_tuple. The no-side-effect failure paths
  // below leave the page untouched, so holding it there is harmless.
  auto barrier = acquire_checkpoint_barrier(checkpoint_barrier);

  TablePage table_page(page);
  if (table_page.update_record(rid.slot_id, tuple.data(),
                               static_cast<uint16_t>(tuple.size()))) {
    if (on_logged) {
      on_logged();
    }
    buffer_pool_->unpin_page(rid.page_id, true);
    return Status::Ok();
  }

  // Distinguish "row is gone" from "does not fit here" so callers can react
  // (propagate vs. drive a relocation). No side effects on either path.
  const bool missing = table_page.get_record(rid.slot_id).empty();
  buffer_pool_->unpin_page(rid.page_id, false);
  return missing ? Status::NotFound("Tuple not found at specified RID")
                 : Status::OutOfMemory("Tuple does not fit in place");
}

Status TableHeap::get_tuple(const RID &rid, Tuple *tuple) {
  std::shared_lock lock(mutex_); // Shared lock for read

  if (tuple == nullptr) {
    return Status::InvalidArgument("Tuple output pointer is null");
  }

  if (!rid.is_valid()) {
    return Status::InvalidArgument("Invalid RID");
  }

  Page *page = buffer_pool_->fetch_page(rid.page_id);
  if (page == nullptr) {
    return Status::NotFound("Page not found");
  }

  TablePage table_page(page);
  auto record = table_page.get_record(rid.slot_id);

  if (record.empty()) {
    buffer_pool_->unpin_page(rid.page_id, false);
    return Status::NotFound("Tuple not found at specified RID");
  }

  // Construct the tuple from raw data
  std::vector<char> data(record.begin(), record.end());
  *tuple = Tuple(std::move(data), rid);

  buffer_pool_->unpin_page(rid.page_id, false);

  return Status::Ok();
}

std::unordered_set<page_id_t> TableHeap::page_ids() const {
  std::shared_lock lock(mutex_);

  std::unordered_set<page_id_t> ids;
  page_id_t current_id = first_page_id_;
  while (current_id != INVALID_PAGE_ID && !ids.contains(current_id)) {
    ids.insert(current_id);

    Page *page = buffer_pool_->fetch_page(current_id);
    if (page == nullptr) {
      break;
    }
    page_id_t next_id = INVALID_PAGE_ID;
    if (page->page_type() == PageType::TABLE_PAGE) {
      next_id = TablePage(page).get_next_page_id();
    }
    buffer_pool_->unpin_page(current_id, false);
    current_id = next_id;
  }
  return ids;
}

TableIterator TableHeap::begin(bool include_empty_slots) {
  if (first_page_id_ == INVALID_PAGE_ID) {
    return end();
  }

  // Start at slot 0 of the first page
  RID start_rid(first_page_id_, 0);
  return TableIterator(this, start_rid, include_empty_slots);
}

TableIterator TableHeap::end() { return TableIterator(); }

page_id_t TableHeap::create_new_page() {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_->new_page(&new_page_id);
  if (new_page == nullptr) {
    return INVALID_PAGE_ID;
  }

  // Initialize the new page as a table page
  TablePage table_page(new_page);
  table_page.init();

  // Link to existing pages
  if (first_page_id_ == INVALID_PAGE_ID) {
    // This is the first page
    first_page_id_ = new_page_id;
    last_page_id_ = new_page_id;
  } else {
    // Append to the end of the list
    // First, update the current last page to point to the new page
    Page *last_page = buffer_pool_->fetch_page(last_page_id_);
    if (last_page != nullptr) {
      TablePage last_table_page(last_page);
      last_table_page.set_next_page_id(new_page_id);
      buffer_pool_->unpin_page(last_page_id_, true);
    }

    // Set the new page's prev link
    table_page.set_prev_page_id(last_page_id_);
    last_page_id_ = new_page_id;
  }

  buffer_pool_->unpin_page(new_page_id, true);

  return new_page_id;
}

page_id_t TableHeap::find_page_with_space(uint32_t size,
                                          const SlotReservedFn &slot_reserved) {
  // Fast path: append to the last page if it has space.
  if (last_page_id_ != INVALID_PAGE_ID) {
    Page *page = buffer_pool_->fetch_page(last_page_id_);
    if (page != nullptr) {
      TablePage table_page(page);
      if (table_page.can_fit(static_cast<uint16_t>(size), slot_reserved)) {
        buffer_pool_->unpin_page(last_page_id_, false);
        return last_page_id_;
      }
      buffer_pool_->unpin_page(last_page_id_, false);
    }
  }

  // Fallback: linear scan through all pages to reuse free space.
  // A more sophisticated implementation would use a free space map.
  std::unordered_set<page_id_t> visited;
  page_id_t current_id = first_page_id_;

  while (current_id != INVALID_PAGE_ID && !visited.contains(current_id)) {
    visited.insert(current_id);
    Page *page = buffer_pool_->fetch_page(current_id);
    if (page == nullptr) {
      break;
    }
    if (page->page_type() != PageType::TABLE_PAGE) {
      // Damaged chain (see the corruption-tolerant walk in the constructor).
      buffer_pool_->unpin_page(current_id, false);
      break;
    }

    TablePage table_page(page);

    // Check if this page has enough space
    // Need space for the record + slot
    if (table_page.can_fit(static_cast<uint16_t>(size), slot_reserved)) {
      buffer_pool_->unpin_page(current_id, false);
      return current_id;
    }

    page_id_t next_id = table_page.get_next_page_id();
    buffer_pool_->unpin_page(current_id, false);
    current_id = next_id;
  }

  return INVALID_PAGE_ID;
}

// ─────────────────────────────────────────────────────────────────────────────
// TableIterator Implementation
// ─────────────────────────────────────────────────────────────────────────────

TableIterator::TableIterator(TableHeap *table_heap, RID rid,
                             bool include_empty_slots)
    : table_heap_(table_heap), rid_(rid),
      include_empty_slots_(include_empty_slots) {
  if (table_heap_) {
    buffer_pool_ = table_heap_->buffer_pool();
  }
  // Find the first valid tuple
  advance_to_next_valid();
}

TableIterator::TableIterator(const TableIterator &other)
    : table_heap_(other.table_heap_),
      rid_(other.rid_),
      include_empty_slots_(other.include_empty_slots_),
      current_tuple_(other.current_tuple_),
      buffer_pool_(other.buffer_pool_) {}

TableIterator &TableIterator::operator=(const TableIterator &other) {
  if (this == &other) {
    return *this;
  }
  release_page();
  table_heap_ = other.table_heap_;
  rid_ = other.rid_;
  include_empty_slots_ = other.include_empty_slots_;
  current_tuple_ = other.current_tuple_;
  buffer_pool_ = other.buffer_pool_;
  pinned_page_ = nullptr;
  pinned_page_id_ = INVALID_PAGE_ID;
  owns_pin_ = false;
  return *this;
}

TableIterator::TableIterator(TableIterator &&other) noexcept
    : table_heap_(other.table_heap_),
      rid_(other.rid_),
      include_empty_slots_(other.include_empty_slots_),
      current_tuple_(std::move(other.current_tuple_)),
      buffer_pool_(std::move(other.buffer_pool_)),
      pinned_page_(other.pinned_page_),
      pinned_page_id_(other.pinned_page_id_),
      owns_pin_(other.owns_pin_) {
  other.table_heap_ = nullptr;
  other.rid_ = RID();
  other.pinned_page_ = nullptr;
  other.pinned_page_id_ = INVALID_PAGE_ID;
  other.owns_pin_ = false;
}

TableIterator &TableIterator::operator=(TableIterator &&other) noexcept {
  if (this == &other) {
    return *this;
  }
  release_page();
  table_heap_ = other.table_heap_;
  rid_ = other.rid_;
  include_empty_slots_ = other.include_empty_slots_;
  current_tuple_ = std::move(other.current_tuple_);
  buffer_pool_ = std::move(other.buffer_pool_);
  pinned_page_ = other.pinned_page_;
  pinned_page_id_ = other.pinned_page_id_;
  owns_pin_ = other.owns_pin_;

  other.table_heap_ = nullptr;
  other.rid_ = RID();
  other.pinned_page_ = nullptr;
  other.pinned_page_id_ = INVALID_PAGE_ID;
  other.owns_pin_ = false;
  return *this;
}

TableIterator::~TableIterator() { release_page(); }

TableIterator &TableIterator::operator++() {
  if (!rid_.is_valid()) {
    return *this;
  }

  // Move to next slot
  rid_.slot_id++;
  advance_to_next_valid();

  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator tmp = *this;
  ++(*this);
  return tmp;
}

void TableIterator::advance_to_next_valid() {
  if (table_heap_ == nullptr) {
    release_page();
    rid_ = RID();
    return;
  }

  if (!buffer_pool_) {
    buffer_pool_ = table_heap_->buffer_pool();
  }

  // Shared heap lock for the whole advance: a writer registers a new row's
  // MVCC version while holding the exclusive lock (insert_tuple's
  // before_publish hook), so under this lock a slot's bytes and its version
  // metadata appear together or not at all. Released before returning control
  // to the executor.
  std::shared_lock<std::shared_mutex> heap_lock(table_heap_->mutex_);

  while (rid_.page_id != INVALID_PAGE_ID) {
    if (pinned_page_id_ != rid_.page_id || pinned_page_ == nullptr) {
      release_page();
      pinned_page_ = buffer_pool_->fetch_page(rid_.page_id);
      if (pinned_page_ == nullptr) {
        rid_ = RID();
        return;
      }
      pinned_page_id_ = rid_.page_id;
      owns_pin_ = true;
    }

    if (pinned_page_ == nullptr) {
      rid_ = RID();
      return;
    }

    if (pinned_page_->page_type() != PageType::TABLE_PAGE) {
      // Damaged chain — stop the scan rather than read garbage slots.
      release_page();
      rid_ = RID();
      return;
    }

    TablePage table_page(pinned_page_);
    uint16_t slot_count = table_page.get_slot_count();

    // Search for a valid slot on this page
    while (rid_.slot_id < slot_count) {
      auto record = table_page.get_record(rid_.slot_id);
      if (!record.empty()) {
        // Found a valid record
        std::vector<char> data(record.begin(), record.end());
        current_tuple_ = Tuple(std::move(data), rid_);
        return;
      }
      if (include_empty_slots_) {
        // Ghost mode: yield the empty slot as an empty tuple with a valid
        // RID so an MVCC scan can ask the version store whether a retained
        // before-image is still visible to its snapshot.
        current_tuple_ = Tuple(std::vector<char>{}, rid_);
        return;
      }
      rid_.slot_id++;
    }

    // No more valid slots on this page, move to next page
    page_id_t next_page_id = table_page.get_next_page_id();
    release_page();

    if (next_page_id == INVALID_PAGE_ID || next_page_id == rid_.page_id) {
      // End of table (a self-link means a damaged chain)
      rid_ = RID();
      return;
    }

    rid_.page_id = next_page_id;
    rid_.slot_id = 0;
  }

  // No valid tuple found
  rid_ = RID();
}

void TableIterator::release_page() {
  if (owns_pin_ && pinned_page_id_ != INVALID_PAGE_ID && buffer_pool_) {
    buffer_pool_->unpin_page(pinned_page_id_, false);
  }
  pinned_page_id_ = INVALID_PAGE_ID;
  pinned_page_ = nullptr;
  owns_pin_ = false;
}

} // namespace entropy
