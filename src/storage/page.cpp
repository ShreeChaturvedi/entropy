/**
 * @file page.cpp
 * @brief Page implementation
 */

#include "storage/page.hpp"

#include <cstring>

namespace entropy {

Page::Page() {
    reset();
}

page_id_t Page::page_id() const noexcept {
    return header()->page_id;
}

void Page::set_page_id(page_id_t page_id) noexcept {
    header()->page_id = page_id;
}

PageType Page::page_type() const noexcept {
    return header()->page_type;
}

void Page::set_page_type(PageType type) noexcept {
    header()->page_type = type;
}

lsn_t Page::lsn() const noexcept {
    return header()->lsn;
}

void Page::set_lsn(lsn_t lsn) noexcept {
    header()->lsn = lsn;
}

void Page::reset() {
    std::memset(data_.data(), 0, data_.size());
    is_dirty_ = false;
    pin_count_ = 0;

    auto* hdr = header();
    hdr->page_id = INVALID_PAGE_ID;
    hdr->page_type = PageType::INVALID;
    hdr->free_space_offset = config::kPageHeaderSize;
    hdr->free_space_end = static_cast<uint16_t>(config::kDefaultPageSize);
}

}  // namespace entropy
