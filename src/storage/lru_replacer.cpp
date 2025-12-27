/**
 * @file lru_replacer.cpp
 * @brief LRU Replacer implementation
 */

#include "storage/lru_replacer.hpp"

namespace entropy {

LRUReplacer::LRUReplacer([[maybe_unused]] size_t num_frames) {}

bool LRUReplacer::evict(frame_id_t* frame_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (lru_list_.empty()) {
        return false;
    }

    // Evict from back (least recently used)
    *frame_id = lru_list_.back();
    lru_list_.pop_back();
    frame_map_.erase(*frame_id);

    return true;
}

void LRUReplacer::pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = frame_map_.find(frame_id);
    if (it != frame_map_.end()) {
        lru_list_.erase(it->second);
        frame_map_.erase(it);
    }
}

void LRUReplacer::unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Don't add if already in replacer
    if (frame_map_.find(frame_id) != frame_map_.end()) {
        return;
    }

    // Add to front (most recently used)
    lru_list_.push_front(frame_id);
    frame_map_[frame_id] = lru_list_.begin();
}

size_t LRUReplacer::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lru_list_.size();
}

}  // namespace entropy
