#pragma once

/**
 * @file page.hpp
 * @brief Page structure definition
 */

#include <array>
#include <cstdint>
#include <cstring>

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
 */
struct PageHeader {
    page_id_t page_id = INVALID_PAGE_ID;    // 4 bytes
    PageType page_type = PageType::INVALID; // 1 byte
    uint8_t reserved1 = 0;                  // 1 byte
    uint16_t record_count = 0;              // 2 bytes
    uint16_t free_space_offset = 0;         // 2 bytes
    uint16_t free_space_end = 0;            // 2 bytes
    lsn_t lsn = INVALID_LSN;                // 8 bytes
    uint32_t checksum = 0;                  // 4 bytes
    uint8_t reserved2[8] = {};              // 8 bytes padding
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
};

}  // namespace entropy
