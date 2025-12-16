#pragma once

/**
 * @file table_page.hpp
 * @brief Slotted page format for table storage
 *
 * Layout:
 * +------------------+  offset 0
 * | Page Header      |  32 bytes
 * +------------------+  offset 32
 * | Slot Array       |  Grows DOWN (toward higher offsets)
 * | [slot_0]         |  Each slot: 4 bytes (offset:2, length:2)
 * | [slot_1]         |
 * | ...              |
 * +------------------+  free_space_offset
 * |                  |
 * | Free Space       |
 * |                  |
 * +------------------+  free_space_end
 * | Records          |  Grows UP (toward lower offsets)
 * | [record_n]       |
 * | ...              |
 * | [record_0]       |
 * +------------------+  PAGE_SIZE
 */

#include <cstdint>
#include <cstring>
#include <optional>
#include <span>

#include "common/config.hpp"
#include "common/types.hpp"
#include "storage/page.hpp"

namespace entropy {

/**
 * @brief Slot directory entry
 *
 * Each slot points to a record within the page.
 * A slot with offset=0 indicates a deleted record.
 */
struct Slot {
    static constexpr uint16_t INVALID_OFFSET = 0;

    uint16_t offset = INVALID_OFFSET;  // Offset from page start (0 = deleted)
    uint16_t length = 0;               // Record length in bytes

    [[nodiscard]] bool is_empty() const noexcept {
        return offset == INVALID_OFFSET;
    }
};

static_assert(sizeof(Slot) == 4, "Slot must be 4 bytes");

/**
 * @brief Table page with slotted page format
 *
 * Provides record-level operations on a page:
 * - Insert variable-length records
 * - Delete records (marks slot as empty)
 * - Update records in place
 * - Get record by slot ID
 */
class TablePage {
public:
    /// Size of the slot directory entry
    static constexpr size_t kSlotSize = sizeof(Slot);

    /// Minimum record size (to avoid fragmentation)
    static constexpr size_t kMinRecordSize = 8;

    /**
     * @brief Initialize a table page from raw page data
     * @param page Pointer to the underlying page
     */
    explicit TablePage(Page* page);

    /**
     * @brief Initialize a new empty table page
     *
     * Sets up the header for an empty slotted page.
     */
    void init();

    // ─────────────────────────────────────────────────────────────────────────
    // Record Operations
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Insert a record into the page
     * @param data Pointer to record data
     * @param size Size of record in bytes
     * @return Slot ID if successful, nullopt if no space
     */
    [[nodiscard]] std::optional<slot_id_t> insert_record(const char* data, uint16_t size);

    /**
     * @brief Delete a record by slot ID
     * @param slot_id The slot to delete
     * @return true if deleted, false if slot was invalid/empty
     */
    bool delete_record(slot_id_t slot_id);

    /**
     * @brief Update a record in place
     * @param slot_id The slot to update
     * @param data New record data
     * @param size New record size
     * @return true if updated, false if no space or invalid slot
     *
     * Note: If new size > old size and doesn't fit, returns false.
     * Caller should delete + insert in that case.
     */
    bool update_record(slot_id_t slot_id, const char* data, uint16_t size);

    /**
     * @brief Get a record by slot ID
     * @param slot_id The slot to read
     * @return Span of record data, or empty span if invalid/deleted
     */
    [[nodiscard]] std::span<const char> get_record(slot_id_t slot_id) const;

    /**
     * @brief Get a mutable reference to record data
     * @param slot_id The slot to access
     * @return Span of record data, or empty span if invalid/deleted
     */
    [[nodiscard]] std::span<char> get_record_mut(slot_id_t slot_id);

    // ─────────────────────────────────────────────────────────────────────────
    // Space Management
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Get the amount of free space in the page
     * @return Free space in bytes (contiguous)
     */
    [[nodiscard]] uint16_t get_free_space() const noexcept;

    /**
     * @brief Check if a record of given size can fit
     * @param size Record size in bytes
     * @return true if record can be inserted
     */
    [[nodiscard]] bool can_fit(uint16_t size) const noexcept;

    /**
     * @brief Get the number of slots (including deleted)
     */
    [[nodiscard]] uint16_t get_slot_count() const noexcept;

    /**
     * @brief Get the number of active (non-deleted) records
     */
    [[nodiscard]] uint16_t get_record_count() const noexcept;

    /**
     * @brief Compact the page to reclaim space from deleted records
     *
     * Moves all records to be contiguous at the end of the page.
     * Updates slot offsets accordingly.
     */
    void compact();

    // ─────────────────────────────────────────────────────────────────────────
    // Page Linking (for heap file)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Get the next page ID in the heap
     */
    [[nodiscard]] page_id_t get_next_page_id() const noexcept;

    /**
     * @brief Set the next page ID in the heap
     */
    void set_next_page_id(page_id_t page_id) noexcept;

    /**
     * @brief Get the previous page ID in the heap
     */
    [[nodiscard]] page_id_t get_prev_page_id() const noexcept;

    /**
     * @brief Set the previous page ID in the heap
     */
    void set_prev_page_id(page_id_t page_id) noexcept;

    // ─────────────────────────────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────────────────────────────

    [[nodiscard]] page_id_t page_id() const noexcept {
        return page_->page_id();
    }

    [[nodiscard]] Page* page() noexcept { return page_; }
    [[nodiscard]] const Page* page() const noexcept { return page_; }

private:
    /// Get slot at index
    [[nodiscard]] Slot* get_slot(slot_id_t slot_id) noexcept;
    [[nodiscard]] const Slot* get_slot(slot_id_t slot_id) const noexcept;

    /// Get the offset where slot array ends
    [[nodiscard]] uint16_t get_slot_array_end() const noexcept;

    /// Set free space pointers
    void set_free_space_offset(uint16_t offset) noexcept;
    void set_free_space_end(uint16_t offset) noexcept;
    void set_slot_count(uint16_t count) noexcept;

    /// Find a free slot (deleted or new)
    [[nodiscard]] slot_id_t find_free_slot();

    Page* page_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Extended Page Header for Table Pages
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Additional header data stored in the reserved bytes
 *
 * The PageHeader has 9 reserved bytes. We use some of them for:
 * - next_page_id (4 bytes) - for heap file linking
 * - prev_page_id (4 bytes) - for heap file linking
 */
struct TablePageHeader {
    static constexpr size_t kNextPageOffset = 23;  // After reserved[0]
    static constexpr size_t kPrevPageOffset = 27;  // After next_page_id

    // Note: These are stored in the reserved bytes of PageHeader
};

}  // namespace entropy
