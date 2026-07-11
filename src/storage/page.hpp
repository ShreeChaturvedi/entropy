#pragma once

/**
 * @file page.hpp
 * @brief Page structure definition
 */

#include <array>
#include <cstddef>
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

// ─────────────────────────────────────────────────────────────────────────────
// Page integrity (checksums)
//
// A page's 4-byte header checksum (offset 12) lets the disk manager DETECT a
// torn/partial write at read time instead of silently returning corrupt bytes.
// The disk manager computes it over the whole page image on write and re-checks
// it on read; a mismatch is surfaced as a Corruption Status.
//
// The checksum occupies bytes [12, 16) of the common 32-byte PageHeader, which
// EVERY page carries at offset 0. That 4-byte slot is the one region no page
// type overlays with payload, so stamping it corrupts nothing:
//   - table heap / catalog data pages keep their slotted-page fields at [16, 22)
//     and their heap next/prev links in reserved bytes [23, 31);
//   - B+ tree internal and leaf nodes place their own BPTreeHeader at
//     kPageHeaderSize (offset 32), so parent_page_id sits at offset 44, not 12,
//     and their keys/children begin at offset 48 — bytes [12, 16) are untouched;
//   - lsn [0, 8) and page_id [8, 12) precede the slot and are read, not payload.
// Because the slot is universally free, both backends stamp/verify it on every
// page: SimDiskManager always, and FileDiskManager by default (see disk
// manager). Recovery treats a detected torn page as needing WAL redo: its
// contents are discarded and rebuilt from the log.
// ─────────────────────────────────────────────────────────────────────────────

/// Byte offset of PageHeader::checksum within a page image.
inline constexpr size_t kPageChecksumOffset = 12;
static_assert(offsetof(PageHeader, checksum) == kPageChecksumOffset,
              "checksum must live at offset 12 for the disk-manager stamp");

namespace detail {

/// One incremental step of a reflected CRC-32 (IEEE polynomial 0xEDB88320),
/// branchless so it is constant-time and free of undefined behaviour. Caller
/// supplies the running value (pre-inverted) and inverts the final result.
[[nodiscard]] inline uint32_t crc32_update(uint32_t crc, const unsigned char* p,
                                           size_t n) noexcept {
    for (size_t i = 0; i < n; ++i) {
        crc ^= p[i];
        for (int k = 0; k < 8; ++k) {
            const uint32_t mask = 0u - (crc & 1u);  // 0x00000000 or 0xFFFFFFFF
            crc = (crc >> 1) ^ (0xEDB88320u & mask);
        }
    }
    return crc;
}

}  // namespace detail

/// CRC-32 over a full page image with the 4-byte checksum field itself treated
/// as zero, so the value never depends on whatever currently sits in that field
/// (write can stamp it, read can re-check it, and both agree).
[[nodiscard]] inline uint32_t compute_page_checksum(const char* page_data) noexcept {
    const auto* p = reinterpret_cast<const unsigned char*>(page_data);
    const unsigned char zeros[4] = {0, 0, 0, 0};
    uint32_t crc = 0xFFFFFFFFu;
    crc = detail::crc32_update(crc, p, kPageChecksumOffset);           // [0, 12)
    crc = detail::crc32_update(crc, zeros, sizeof(zeros));             // field -> 0
    crc = detail::crc32_update(crc, p + kPageChecksumOffset + 4,       // [16, end)
                               config::kDefaultPageSize - kPageChecksumOffset - 4);
    return crc ^ 0xFFFFFFFFu;
}

/// Stamp the computed checksum into the page header (offset 12). @p page_data
/// must point at a full kDefaultPageSize image. Uses memcpy to avoid any
/// alignment/strict-aliasing assumption on the raw byte buffer.
inline void stamp_page_checksum(char* page_data) noexcept {
    const uint32_t sum = compute_page_checksum(page_data);
    std::memcpy(page_data + kPageChecksumOffset, &sum, sizeof(sum));
}

/// True when every byte of the page image is zero. A freshly allocated or
/// deallocated page is all-zero and carries no integrity stamp; it is a valid
/// EMPTY page, not a corruption, so verification accepts it.
[[nodiscard]] inline bool page_is_all_zero(const char* page_data) noexcept {
    const auto* p = reinterpret_cast<const unsigned char*>(page_data);
    for (size_t i = 0; i < config::kDefaultPageSize; ++i) {
        if (p[i] != 0) {
            return false;
        }
    }
    return true;
}

/// Verify a page image against its stored checksum. An all-zero page is a valid
/// empty page (accepted). Otherwise the stored checksum (offset 12) must equal
/// the recomputed value; any mismatch — including a torn page whose header was
/// zeroed while its body kept data, which reads as checksum 0 over non-zero
/// bytes — signals a torn/corrupt page. Returns true iff the page is trustworthy.
[[nodiscard]] inline bool verify_page_checksum(const char* page_data) noexcept {
    if (page_is_all_zero(page_data)) {
        return true;
    }
    uint32_t stored = 0;
    std::memcpy(&stored, page_data + kPageChecksumOffset, sizeof(stored));
    return stored == compute_page_checksum(page_data);
}

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
