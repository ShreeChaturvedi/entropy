/**
 * @file table_heap.cpp
 * @brief Table Heap implementation
 */

#include "storage/table_heap.hpp"

#include <cstring>

#include "common/config.hpp"
#include "common/status.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// TableHeap Implementation
// ─────────────────────────────────────────────────────────────────────────────

TableHeap::TableHeap(std::shared_ptr<BufferPoolManager> buffer_pool)
    : buffer_pool_(std::move(buffer_pool)) {}

TableHeap::TableHeap(std::shared_ptr<BufferPoolManager> buffer_pool,
                     page_id_t first_page_id)
    : buffer_pool_(std::move(buffer_pool)), first_page_id_(first_page_id) {
  // Find the last page by traversing the linked list
  if (first_page_id_ != INVALID_PAGE_ID) {
    page_id_t current_id = first_page_id_;
    while (current_id != INVALID_PAGE_ID) {
      Page *page = buffer_pool_->fetch_page(current_id);
      if (page == nullptr) {
        break;
      }
      TablePage table_page(page);
      page_id_t next_id = table_page.get_next_page_id();
      if (next_id == INVALID_PAGE_ID) {
        last_page_id_ = current_id;
      }
      buffer_pool_->unpin_page(current_id, false);
      current_id = next_id;
    }
  }
}

Status TableHeap::insert_tuple(const Tuple &tuple, RID *rid) {
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
  page_id_t target_page_id = find_page_with_space(tuple_size);

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

  TablePage table_page(page);
  auto slot_id =
      table_page.insert_record(tuple.data(), static_cast<uint16_t>(tuple_size));

  if (!slot_id.has_value()) {
    // This shouldn't happen if find_page_with_space worked correctly
    buffer_pool_->unpin_page(target_page_id, false);
    return Status::Internal(
        "Failed to insert tuple into page with reported space");
  }

  // Set the RID
  rid->page_id = target_page_id;
  rid->slot_id = slot_id.value();

  buffer_pool_->unpin_page(target_page_id, true); // Mark as dirty

  return Status::Ok();
}

Status TableHeap::delete_tuple(const RID &rid) {
  std::unique_lock lock(mutex_); // Exclusive lock for write

  if (!rid.is_valid()) {
    return Status::InvalidArgument("Invalid RID");
  }

  Page *page = buffer_pool_->fetch_page(rid.page_id);
  if (page == nullptr) {
    return Status::NotFound("Page not found");
  }

  TablePage table_page(page);
  bool deleted = table_page.delete_record(rid.slot_id);

  buffer_pool_->unpin_page(rid.page_id, deleted); // Mark dirty if deleted

  if (!deleted) {
    return Status::NotFound("Tuple not found at specified RID");
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

TableIterator TableHeap::begin() {
  if (first_page_id_ == INVALID_PAGE_ID) {
    return end();
  }

  // Start at slot 0 of the first page
  RID start_rid(first_page_id_, 0);
  return TableIterator(this, start_rid);
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

page_id_t TableHeap::find_page_with_space(uint32_t size) {
  // Simple linear scan through all pages
  // A more sophisticated implementation would use a free space map
  page_id_t current_id = first_page_id_;

  while (current_id != INVALID_PAGE_ID) {
    Page *page = buffer_pool_->fetch_page(current_id);
    if (page == nullptr) {
      break;
    }

    TablePage table_page(page);

    // Check if this page has enough space
    // Need space for the record + slot
    if (table_page.can_fit(static_cast<uint16_t>(size))) {
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

TableIterator::TableIterator(TableHeap *table_heap, RID rid)
    : table_heap_(table_heap), rid_(rid) {
  // Find the first valid tuple
  advance_to_next_valid();
}

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
    rid_ = RID();
    return;
  }

  auto buffer_pool = table_heap_->buffer_pool();

  while (rid_.page_id != INVALID_PAGE_ID) {
    Page *page = buffer_pool->fetch_page(rid_.page_id);
    if (page == nullptr) {
      rid_ = RID();
      return;
    }

    TablePage table_page(page);
    uint16_t slot_count = table_page.get_slot_count();

    // Search for a valid slot on this page
    while (rid_.slot_id < slot_count) {
      auto record = table_page.get_record(rid_.slot_id);
      if (!record.empty()) {
        // Found a valid record
        std::vector<char> data(record.begin(), record.end());
        current_tuple_ = Tuple(std::move(data), rid_);
        buffer_pool->unpin_page(rid_.page_id, false);
        return;
      }
      rid_.slot_id++;
    }

    // No more valid slots on this page, move to next page
    page_id_t next_page_id = table_page.get_next_page_id();
    buffer_pool->unpin_page(rid_.page_id, false);

    if (next_page_id == INVALID_PAGE_ID) {
      // End of table
      rid_ = RID();
      return;
    }

    rid_.page_id = next_page_id;
    rid_.slot_id = 0;
  }

  // No valid tuple found
  rid_ = RID();
}

} // namespace entropy
