#pragma once

/**
 * @file lru_replacer.hpp
 * @brief LRU eviction policy implementation
 */

#include <list>
#include <mutex>
#include <unordered_map>

#include "common/types.hpp"

namespace entropy {

/**
 * @brief LRU Replacer for buffer pool eviction
 *
 * Tracks which frames are eligible for eviction and selects victims
 * using the Least Recently Used policy.
 */
class LRUReplacer {
public:
    /**
     * @brief Construct an LRU Replacer
     * @param num_frames Maximum number of frames to track
     */
    explicit LRUReplacer(size_t num_frames);

    ~LRUReplacer() = default;

    /**
     * @brief Remove the least recently used frame
     * @param frame_id Output parameter for the evicted frame
     * @return true if a frame was evicted, false if replacer is empty
     */
    bool evict(frame_id_t* frame_id);

    /**
     * @brief Mark a frame as recently used (remove from replacer)
     * @param frame_id The frame to pin
     */
    void pin(frame_id_t frame_id);

    /**
     * @brief Mark a frame as eligible for eviction
     * @param frame_id The frame to unpin
     */
    void unpin(frame_id_t frame_id);

    /**
     * @brief Get the number of frames eligible for eviction
     */
    [[nodiscard]] size_t size() const;

private:
    std::list<frame_id_t> lru_list_;
    std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> frame_map_;
    mutable std::mutex mutex_;
};

}  // namespace entropy
