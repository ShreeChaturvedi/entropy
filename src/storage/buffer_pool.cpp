/**
 * @file buffer_pool.cpp
 * @brief Buffer Pool Manager implementation
 */

#include "storage/buffer_pool.hpp"

#include "common/logger.hpp"

namespace entropy {

BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     std::shared_ptr<DiskManager> disk_manager)
    : pool_size_(pool_size),
      disk_manager_(std::move(disk_manager)),
      pages_(pool_size),
      replacer_(std::make_unique<LRUReplacer>(pool_size)) {
    // Initialize free list with all frames
    free_list_.reserve(pool_size);
    for (size_t i = 0; i < pool_size; ++i) {
        free_list_.push_back(static_cast<frame_id_t>(i));
    }
}

BufferPoolManager::~BufferPoolManager() {
    flush_all_pages();
}

Page* BufferPoolManager::fetch_page(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check if page is already in buffer pool
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        Page* page = &pages_[frame_id];
        page->pin();
        replacer_->pin(frame_id);
        return page;
    }

    // Need to fetch from disk
    frame_id_t frame_id = find_victim_frame();
    if (frame_id == INVALID_FRAME_ID) {
        LOG_WARN("Buffer pool full, cannot fetch page {}", page_id);
        return nullptr;
    }

    Page* page = &pages_[frame_id];

    // If victim page is dirty, flush it
    if (page->is_dirty()) {
        disk_manager_->write_page(page->page_id(), page->data());
    }

    // Remove old page from page table
    if (page->page_id() != INVALID_PAGE_ID) {
        page_table_.erase(page->page_id());
    }

    // Read new page from disk
    page->reset();
    auto status = disk_manager_->read_page(page_id, page->data());
    if (!status.ok()) {
        LOG_ERROR("Failed to read page {}: {}", page_id, status.to_string());
        free_list_.push_back(frame_id);
        return nullptr;
    }

    page->set_page_id(page_id);
    page->pin();
    page_table_[page_id] = frame_id;
    replacer_->pin(frame_id);

    return page;
}

bool BufferPoolManager::unpin_page(page_id_t page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }

    frame_id_t frame_id = it->second;
    Page* page = &pages_[frame_id];

    if (is_dirty) {
        page->set_dirty(true);
    }

    if (page->pin_count() <= 0) {
        return false;
    }

    page->unpin();

    if (page->pin_count() == 0) {
        replacer_->unpin(frame_id);
    }

    return true;
}

bool BufferPoolManager::flush_page(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }

    frame_id_t frame_id = it->second;
    Page* page = &pages_[frame_id];

    auto status = disk_manager_->write_page(page_id, page->data());
    if (!status.ok()) {
        return false;
    }

    page->set_dirty(false);
    return true;
}

Page* BufferPoolManager::new_page(page_id_t* page_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    frame_id_t frame_id = find_victim_frame();
    if (frame_id == INVALID_FRAME_ID) {
        return nullptr;
    }

    Page* page = &pages_[frame_id];

    // Flush victim if dirty
    if (page->is_dirty()) {
        disk_manager_->write_page(page->page_id(), page->data());
    }

    // Remove old page from page table
    if (page->page_id() != INVALID_PAGE_ID) {
        page_table_.erase(page->page_id());
    }

    // Allocate new page
    *page_id = disk_manager_->allocate_page();

    page->reset();
    page->set_page_id(*page_id);
    page->pin();
    page_table_[*page_id] = frame_id;
    replacer_->pin(frame_id);

    return page;
}

bool BufferPoolManager::delete_page(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return true;  // Page not in buffer pool
    }

    frame_id_t frame_id = it->second;
    Page* page = &pages_[frame_id];

    if (page->pin_count() > 0) {
        return false;  // Page is pinned
    }

    page_table_.erase(page_id);
    replacer_->pin(frame_id);  // Remove from replacer
    page->reset();
    free_list_.push_back(frame_id);

    disk_manager_->deallocate_page(page_id);
    return true;
}

void BufferPoolManager::flush_all_pages() {
    std::lock_guard<std::mutex> lock(mutex_);

    for (auto& [page_id, frame_id] : page_table_) {
        Page* page = &pages_[frame_id];
        if (page->is_dirty()) {
            disk_manager_->write_page(page_id, page->data());
            page->set_dirty(false);
        }
    }
}

frame_id_t BufferPoolManager::find_victim_frame() {
    // First, check free list
    if (!free_list_.empty()) {
        frame_id_t frame_id = free_list_.back();
        free_list_.pop_back();
        return frame_id;
    }

    // Try to evict a page
    frame_id_t frame_id;
    if (replacer_->evict(&frame_id)) {
        return frame_id;
    }

    return INVALID_FRAME_ID;
}

}  // namespace entropy
