#pragma once

/**
 * @file page.hpp
 * @brief Page structure definition
 */

#include <array>
#include <cstdint>
#include <cstring>
#include <shared_mutex>

#include "common/config.hpp"
#include "common/types.hpp"

namespace entropy {

/**
 * @brief Page types
 */
enum class PageType : uint8_t {
    INVALID = 0,
    TABLE_PAGE,
    INDEX_INTERNAL_PAGE,
    INDEX_LEAF_PAGE,
    HEADER_PAGE,
    FREE_PAGE,
};

/**
 * @brief Page header structure (32 bytes)
 *
 * Fields are ordered to avoid alignment padding:
 * - 8-byte fields first, then 4-byte, 2-byte, 1-byte
 */
struct PageHeader {
    lsn_t lsn = INVALID_LSN;                // 8 bytes (offset 0)
    page_id_t page_id = INVALID_PAGE_ID;    // 4 bytes (offset 8)
    uint32_t checksum = 0;                  // 4 bytes (offset 12)
    uint16_t record_count = 0;              // 2 bytes (offset 16)
    uint16_t free_space_offset = 0;         // 2 bytes (offset 18)
    uint16_t free_space_end = 0;            // 2 bytes (offset 20)
    PageType page_type = PageType::INVALID; // 1 byte  (offset 22)
    uint8_t reserved[9] = {};               // 9 bytes (offset 23-31)
};

static_assert(sizeof(PageHeader) == 32, "PageHeader must be 32 bytes");

/**
 * @brief Base page class
 *
 * Represents a single page of data. Pages are the fundamental unit of
 * storage and are read/written as atomic units.
 */
class Page {
public:
    Page();
    ~Page() = default;

    /// Get the page data
    [[nodiscard]] char* data() noexcept { return data_.data(); }
    [[nodiscard]] const char* data() const noexcept { return data_.data(); }

    /// Get the page ID
    [[nodiscard]] page_id_t page_id() const noexcept;

    /// Set the page ID
    void set_page_id(page_id_t page_id) noexcept;

    /// Get the page type
    [[nodiscard]] PageType page_type() const noexcept;

    /// Set the page type
    void set_page_type(PageType type) noexcept;

    /// Get the LSN
    [[nodiscard]] lsn_t lsn() const noexcept;

    /// Set the LSN
    void set_lsn(lsn_t lsn) noexcept;

    /// Check if page is dirty
    [[nodiscard]] bool is_dirty() const noexcept { return is_dirty_; }

    /// Mark page as dirty
    void set_dirty(bool dirty) noexcept { is_dirty_ = dirty; }

    /// Get pin count
    [[nodiscard]] int pin_count() const noexcept { return pin_count_; }

    /// Increment pin count
    void pin() noexcept { ++pin_count_; }

    /// Decrement pin count
    void unpin() noexcept { if (pin_count_ > 0) --pin_count_; }

    /// Reset the page to initial state
    void reset();

    // ─────────────────────────────────────────────────────────────────────────
    // Page latch (reader/writer)
    //
    // A per-page reader/writer latch used by the B+ tree's latch-crabbing
    // protocol. It guards this frame's contents (data_) against concurrent
    // access. Crucially it is a SEPARATE member from data_: reset() (called by
    // the buffer pool on frame reuse) clears only data_/is_dirty_/pin_count_ and
    // never touches the latch, so the latch object survives frame reuse. A
    // latched page is always kept pinned by its holder, and the buffer pool
    // refuses to evict or delete a pinned frame, so the frame a latch protects
    // cannot be reused out from under the latch holder.
    // ─────────────────────────────────────────────────────────────────────────

    /// Acquire the latch in shared (read) mode.
    void rlatch() { latch_.lock_shared(); }

    /// Release a shared (read) latch.
    void runlatch() { latch_.unlock_shared(); }

    /// Acquire the latch in exclusive (write) mode.
    void wlatch() { latch_.lock(); }

    /// Release an exclusive (write) latch.
    void wunlatch() { latch_.unlock(); }

    /// Get the header
    [[nodiscard]] PageHeader* header() noexcept {
        return reinterpret_cast<PageHeader*>(data_.data());
    }

    [[nodiscard]] const PageHeader* header() const noexcept {
        return reinterpret_cast<const PageHeader*>(data_.data());
    }

    /// Page size constant
    static constexpr size_t kPageSize = config::kDefaultPageSize;

private:
    std::array<char, config::kDefaultPageSize> data_{};
    bool is_dirty_ = false;
    int pin_count_ = 0;
    // Not part of the on-disk page image and intentionally excluded from
    // reset(); see the latch documentation above.
    std::shared_mutex latch_;
};

}  // namespace entropy
