/**
 * @file table_page.cpp
 * @brief Slotted page implementation
 */

#include "storage/table_page.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>

#include "common/macros.hpp"

namespace entropy {

TablePage::TablePage(Page* page) : page_(page) {
    ENTROPY_ASSERT(page_ != nullptr, "Page cannot be null");
}

void TablePage::init() {
    // Set page type
    page_->set_page_type(PageType::TABLE_PAGE);

    // Initialize free space pointers
    // Slot array starts right after header
    auto* header = page_->header();
    header->free_space_offset = static_cast<uint16_t>(config::kPageHeaderSize);
    header->free_space_end = static_cast<uint16_t>(config::kDefaultPageSize);
    header->record_count = 0;

    // Initialize link pointers
    set_next_page_id(INVALID_PAGE_ID);
    set_prev_page_id(INVALID_PAGE_ID);

    page_->set_dirty(true);
}

// ─────────────────────────────────────────────────────────────────────────────
// Record Operations
// ─────────────────────────────────────────────────────────────────────────────

std::optional<slot_id_t> TablePage::insert_record(const char* data, uint16_t size) {
    if (data == nullptr || size == 0) {
        return std::nullopt;
    }

    // Check if we have space for the record + possibly a new slot
    uint16_t space_needed = size;
    slot_id_t slot_id = find_free_slot();

    // If no free slot exists, we need space for a new slot too
    if (slot_id == get_slot_count()) {
        space_needed += kSlotSize;
    }

    if (get_free_space() < space_needed) {
        return std::nullopt;
    }

    // Allocate space for record at the end (growing upward)
    auto* header = page_->header();
    uint16_t record_offset = header->free_space_end - size;

    // Copy record data
    std::memcpy(page_->data() + record_offset, data, size);

    // Update or create slot
    Slot* slot = get_slot(slot_id);
    slot->offset = record_offset;
    slot->length = size;

    // Update header
    header->free_space_end = record_offset;

    // If we used a new slot, update slot count and free_space_offset
    if (slot_id >= header->record_count) {
        header->record_count = slot_id + 1;
        header->free_space_offset = static_cast<uint16_t>(
            config::kPageHeaderSize + (slot_id + 1) * kSlotSize
        );
    }

    page_->set_dirty(true);
    return slot_id;
}

bool TablePage::delete_record(slot_id_t slot_id) {
    if (slot_id >= get_slot_count()) {
        return false;
    }

    Slot* slot = get_slot(slot_id);
    if (slot->is_empty()) {
        return false;  // Already deleted
    }

    // Mark as deleted
    slot->offset = Slot::INVALID_OFFSET;
    slot->length = 0;

    page_->set_dirty(true);
    return true;
}

bool TablePage::update_record(slot_id_t slot_id, const char* data, uint16_t size) {
    if (slot_id >= get_slot_count() || data == nullptr || size == 0) {
        return false;
    }

    Slot* slot = get_slot(slot_id);
    if (slot->is_empty()) {
        return false;
    }

    // If new size fits in existing space, update in place
    if (size <= slot->length) {
        std::memcpy(page_->data() + slot->offset, data, size);
        // Note: We keep the old length to maintain contiguity
        // The extra space becomes internal fragmentation
        page_->set_dirty(true);
        return true;
    }

    // New record is larger - check if we have enough free space
    uint16_t extra_space_needed = size - slot->length;
    if (get_free_space() < extra_space_needed) {
        return false;  // Caller should delete + insert
    }

    // Delete old record and insert at new location
    // First, mark the old slot as empty temporarily
    uint16_t old_offset = slot->offset;
    uint16_t old_length = slot->length;
    slot->offset = Slot::INVALID_OFFSET;

    // Allocate new space
    auto* header = page_->header();
    uint16_t new_offset = header->free_space_end - size;

    // Copy new data
    std::memcpy(page_->data() + new_offset, data, size);

    // Update slot
    slot->offset = new_offset;
    slot->length = size;
    header->free_space_end = new_offset;

    // Note: old space becomes fragmentation, compact() can reclaim it
    (void)old_offset;
    (void)old_length;

    page_->set_dirty(true);
    return true;
}

std::span<const char> TablePage::get_record(slot_id_t slot_id) const {
    if (slot_id >= get_slot_count()) {
        return {};
    }

    const Slot* slot = get_slot(slot_id);
    if (slot->is_empty()) {
        return {};
    }

    return std::span<const char>(
        page_->data() + slot->offset,
        slot->length
    );
}

std::span<char> TablePage::get_record_mut(slot_id_t slot_id) {
    if (slot_id >= get_slot_count()) {
        return {};
    }

    Slot* slot = get_slot(slot_id);
    if (slot->is_empty()) {
        return {};
    }

    page_->set_dirty(true);
    return std::span<char>(
        page_->data() + slot->offset,
        slot->length
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Space Management
// ─────────────────────────────────────────────────────────────────────────────

uint16_t TablePage::get_free_space() const noexcept {
    const auto* header = page_->header();
    if (header->free_space_end <= header->free_space_offset) {
        return 0;
    }
    return header->free_space_end - header->free_space_offset;
}

bool TablePage::can_fit(uint16_t size) const noexcept {
    // Need space for record + possibly a new slot
    uint16_t space_needed = size;

    // Check if there's a free (deleted) slot we can reuse
    bool has_free_slot = false;
    uint16_t slot_count = get_slot_count();
    for (uint16_t i = 0; i < slot_count; ++i) {
        if (get_slot(i)->is_empty()) {
            has_free_slot = true;
            break;
        }
    }

    if (!has_free_slot) {
        space_needed += kSlotSize;
    }

    return get_free_space() >= space_needed;
}

uint16_t TablePage::get_slot_count() const noexcept {
    return page_->header()->record_count;
}

uint16_t TablePage::get_record_count() const noexcept {
    uint16_t count = 0;
    uint16_t slot_count = get_slot_count();
    for (uint16_t i = 0; i < slot_count; ++i) {
        if (!get_slot(i)->is_empty()) {
            ++count;
        }
    }
    return count;
}

void TablePage::compact() {
    uint16_t slot_count = get_slot_count();
    if (slot_count == 0) {
        return;
    }

    // Collect all active records with their slot IDs
    struct RecordInfo {
        slot_id_t slot_id;
        uint16_t offset;
        uint16_t length;
    };

    std::vector<RecordInfo> records;
    records.reserve(slot_count);

    for (uint16_t i = 0; i < slot_count; ++i) {
        const Slot* slot = get_slot(i);
        if (!slot->is_empty()) {
            records.push_back({i, slot->offset, slot->length});
        }
    }

    if (records.empty()) {
        // All slots are empty, reset free_space_end
        page_->header()->free_space_end =
            static_cast<uint16_t>(config::kDefaultPageSize);
        page_->set_dirty(true);
        return;
    }

    // Sort by offset (descending) so we process from end of page
    std::sort(records.begin(), records.end(),
              [](const RecordInfo& a, const RecordInfo& b) {
                  return a.offset > b.offset;
              });

    // Temporary buffer for compaction
    std::vector<char> temp_buffer;
    size_t total_size = 0;
    for (const auto& rec : records) {
        total_size += rec.length;
    }
    temp_buffer.resize(total_size);

    // Copy records to temp buffer
    size_t buffer_offset = 0;
    for (const auto& rec : records) {
        std::memcpy(temp_buffer.data() + buffer_offset,
                    page_->data() + rec.offset,
                    rec.length);
        buffer_offset += rec.length;
    }

    // Write records back contiguously from end of page
    uint16_t write_offset = static_cast<uint16_t>(config::kDefaultPageSize);
    buffer_offset = 0;

    for (auto& rec : records) {
        write_offset -= rec.length;
        std::memcpy(page_->data() + write_offset,
                    temp_buffer.data() + buffer_offset,
                    rec.length);

        // Update slot with new offset
        Slot* slot = get_slot(rec.slot_id);
        slot->offset = write_offset;

        buffer_offset += rec.length;
    }

    // Update free space end
    page_->header()->free_space_end = write_offset;
    page_->set_dirty(true);
}

// ─────────────────────────────────────────────────────────────────────────────
// Page Linking
// ─────────────────────────────────────────────────────────────────────────────

page_id_t TablePage::get_next_page_id() const noexcept {
    page_id_t next_id;
    std::memcpy(&next_id,
                page_->data() + TablePageHeader::kNextPageOffset,
                sizeof(page_id_t));
    return next_id;
}

void TablePage::set_next_page_id(page_id_t page_id) noexcept {
    std::memcpy(page_->data() + TablePageHeader::kNextPageOffset,
                &page_id,
                sizeof(page_id_t));
    page_->set_dirty(true);
}

page_id_t TablePage::get_prev_page_id() const noexcept {
    page_id_t prev_id;
    std::memcpy(&prev_id,
                page_->data() + TablePageHeader::kPrevPageOffset,
                sizeof(page_id_t));
    return prev_id;
}

void TablePage::set_prev_page_id(page_id_t page_id) noexcept {
    std::memcpy(page_->data() + TablePageHeader::kPrevPageOffset,
                &page_id,
                sizeof(page_id_t));
    page_->set_dirty(true);
}

// ─────────────────────────────────────────────────────────────────────────────
// Private Helpers
// ─────────────────────────────────────────────────────────────────────────────

Slot* TablePage::get_slot(slot_id_t slot_id) noexcept {
    char* slot_ptr = page_->data() + config::kPageHeaderSize + slot_id * kSlotSize;
    return reinterpret_cast<Slot*>(slot_ptr);
}

const Slot* TablePage::get_slot(slot_id_t slot_id) const noexcept {
    const char* slot_ptr = page_->data() + config::kPageHeaderSize + slot_id * kSlotSize;
    return reinterpret_cast<const Slot*>(slot_ptr);
}

uint16_t TablePage::get_slot_array_end() const noexcept {
    return page_->header()->free_space_offset;
}

void TablePage::set_free_space_offset(uint16_t offset) noexcept {
    page_->header()->free_space_offset = offset;
}

void TablePage::set_free_space_end(uint16_t offset) noexcept {
    page_->header()->free_space_end = offset;
}

void TablePage::set_slot_count(uint16_t count) noexcept {
    page_->header()->record_count = count;
}

slot_id_t TablePage::find_free_slot() {
    uint16_t slot_count = get_slot_count();

    // First, look for a deleted slot we can reuse
    for (uint16_t i = 0; i < slot_count; ++i) {
        if (get_slot(i)->is_empty()) {
            return i;
        }
    }

    // No free slot, return index for new slot
    return slot_count;
}

}  // namespace entropy
