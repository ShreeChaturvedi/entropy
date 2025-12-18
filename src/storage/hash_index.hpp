#pragma once

/**
 * @file hash_index.hpp
 * @brief Extendible Hash Index for O(1) equality lookups
 *
 * Design Goals:
 * 1. O(1) average case lookups/inserts
 * 2. Dynamic growth via bucket splitting
 * 3. Space-efficient directory structure
 * 4. Concurrent-safe with latch crabbing
 *
 * Structure:
 * - Directory: Array of pointers to buckets, indexed by hash prefix
 * - Buckets: Fixed-size pages holding key-value pairs
 * - Global depth: Number of bits used for directory
 * - Local depth: Number of bits used per bucket
 *
 * Operations:
 * - Insert: Hash key, find bucket, insert or split if full
 * - Find: Hash key, find bucket, linear search
 * - Remove: Hash key, find bucket, remove entry
 */

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <vector>

#include "common/types.hpp"
#include "entropy/status.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Hash Bucket
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief A bucket in the extendible hash table
 *
 * Stores key-value pairs with a local depth for splitting decisions.
 */
template <typename KeyType, typename ValueType> class HashBucket {
public:
  static constexpr size_t MAX_ENTRIES = 256; // Entries per bucket

  explicit HashBucket(uint32_t local_depth = 0) : local_depth_(local_depth) {}

  /**
   * @brief Insert key-value pair
   * @return true if inserted, false if full or duplicate
   */
  bool insert(const KeyType &key, const ValueType &value) {
    // Check for duplicate
    for (const auto &[k, v] : entries_) {
      if (k == key) {
        return false; // Duplicate key
      }
    }
    if (entries_.size() >= MAX_ENTRIES) {
      return false; // Bucket full
    }
    entries_.emplace_back(key, value);
    return true;
  }

  /**
   * @brief Find value by key
   */
  std::optional<ValueType> find(const KeyType &key) const {
    for (const auto &[k, v] : entries_) {
      if (k == key) {
        return v;
      }
    }
    return std::nullopt;
  }

  /**
   * @brief Remove entry by key
   */
  bool remove(const KeyType &key) {
    for (auto it = entries_.begin(); it != entries_.end(); ++it) {
      if (it->first == key) {
        entries_.erase(it);
        return true;
      }
    }
    return false;
  }

  [[nodiscard]] bool is_full() const { return entries_.size() >= MAX_ENTRIES; }
  [[nodiscard]] bool is_empty() const { return entries_.empty(); }
  [[nodiscard]] size_t size() const { return entries_.size(); }
  [[nodiscard]] uint32_t local_depth() const { return local_depth_; }
  void set_local_depth(uint32_t depth) { local_depth_ = depth; }

  // Access entries for splitting
  std::vector<std::pair<KeyType, ValueType>> &entries() { return entries_; }
  const std::vector<std::pair<KeyType, ValueType>> &entries() const {
    return entries_;
  }

private:
  uint32_t local_depth_;
  std::vector<std::pair<KeyType, ValueType>> entries_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Extendible Hash Index
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Extendible Hash Table
 *
 * Uses dynamic directory doubling for growth.
 * Each bucket has a local depth ≤ global depth.
 *
 * Hash function: std::hash for key type
 */
template <typename KeyType, typename ValueType> class HashIndex {
public:
  /**
   * @brief Construct with initial global depth
   * @param initial_depth Initial global depth (2^depth directory entries)
   */
  explicit HashIndex(uint32_t initial_depth = 2);

  ~HashIndex() = default;

  // Disable copy
  HashIndex(const HashIndex &) = delete;
  HashIndex &operator=(const HashIndex &) = delete;

  /**
   * @brief Insert key-value pair
   * @return Ok if inserted, error if duplicate key
   */
  [[nodiscard]] Status insert(const KeyType &key, const ValueType &value);

  /**
   * @brief Remove key
   * @return Ok if removed, NotFound if key doesn't exist
   */
  [[nodiscard]] Status remove(const KeyType &key);

  /**
   * @brief Find value by key
   * @return Value if found, nullopt otherwise
   */
  [[nodiscard]] std::optional<ValueType> find(const KeyType &key) const;

  /**
   * @brief Check if key exists
   */
  [[nodiscard]] bool contains(const KeyType &key) const {
    return find(key).has_value();
  }

  /**
   * @brief Get current global depth
   */
  [[nodiscard]] uint32_t global_depth() const { return global_depth_; }

  /**
   * @brief Get number of directory entries
   */
  [[nodiscard]] size_t directory_size() const { return directory_.size(); }

  /**
   * @brief Get total number of entries
   */
  [[nodiscard]] size_t size() const;

private:
  using BucketPtr = std::shared_ptr<HashBucket<KeyType, ValueType>>;

  /**
   * @brief Get directory index for a key
   */
  size_t get_index(const KeyType &key) const {
    size_t hash = std::hash<KeyType>{}(key);
    return hash & ((1ULL << global_depth_) - 1);
  }

  /**
   * @brief Get bucket for a key
   */
  BucketPtr get_bucket(const KeyType &key) const {
    return directory_[get_index(key)];
  }

  /**
   * @brief Split a full bucket
   */
  void split_bucket(size_t bucket_idx);

  /**
   * @brief Double the directory size
   */
  void grow_directory();

  uint32_t global_depth_;
  std::vector<BucketPtr> directory_;
  mutable std::shared_mutex mutex_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Template Implementation
// ─────────────────────────────────────────────────────────────────────────────

template <typename KeyType, typename ValueType>
HashIndex<KeyType, ValueType>::HashIndex(uint32_t initial_depth)
    : global_depth_(initial_depth) {
  size_t num_buckets = 1ULL << global_depth_;
  directory_.reserve(num_buckets);
  for (size_t i = 0; i < num_buckets; ++i) {
    directory_.push_back(
        std::make_shared<HashBucket<KeyType, ValueType>>(global_depth_));
  }
}

template <typename KeyType, typename ValueType>
Status HashIndex<KeyType, ValueType>::insert(const KeyType &key,
                                             const ValueType &value) {
  std::unique_lock lock(mutex_);

  size_t idx = get_index(key);
  auto bucket = directory_[idx];

  // Check for duplicate
  if (bucket->find(key).has_value()) {
    return Status::InvalidArgument("Duplicate key");
  }

  // Try inserting
  if (bucket->insert(key, value)) {
    return Status::Ok();
  }

  // Bucket is full, need to split
  split_bucket(idx);

  // Retry insert after split
  idx = get_index(key);
  bucket = directory_[idx];
  if (bucket->insert(key, value)) {
    return Status::Ok();
  }

  return Status::InvalidArgument("Failed to insert after split");
}

template <typename KeyType, typename ValueType>
Status HashIndex<KeyType, ValueType>::remove(const KeyType &key) {
  std::unique_lock lock(mutex_);

  auto bucket = get_bucket(key);
  if (bucket->remove(key)) {
    return Status::Ok();
  }
  return Status::NotFound("Key not found");
}

template <typename KeyType, typename ValueType>
std::optional<ValueType>
HashIndex<KeyType, ValueType>::find(const KeyType &key) const {
  std::shared_lock lock(mutex_);
  return get_bucket(key)->find(key);
}

template <typename KeyType, typename ValueType>
size_t HashIndex<KeyType, ValueType>::size() const {
  std::shared_lock lock(mutex_);
  size_t total = 0;
  // Count unique buckets
  std::vector<HashBucket<KeyType, ValueType> *> seen;
  for (const auto &bucket : directory_) {
    bool found = false;
    for (const auto *b : seen) {
      if (b == bucket.get()) {
        found = true;
        break;
      }
    }
    if (!found) {
      seen.push_back(bucket.get());
      total += bucket->size();
    }
  }
  return total;
}

template <typename KeyType, typename ValueType>
void HashIndex<KeyType, ValueType>::split_bucket(size_t bucket_idx) {
  auto old_bucket = directory_[bucket_idx];
  uint32_t local_depth = old_bucket->local_depth();

  // If local depth equals global depth, need to grow directory
  if (local_depth == global_depth_) {
    grow_directory();
  }

  // Create new bucket with increased local depth
  uint32_t new_local_depth = local_depth + 1;
  auto new_bucket =
      std::make_shared<HashBucket<KeyType, ValueType>>(new_local_depth);
  old_bucket->set_local_depth(new_local_depth);

  // Calculate the bit pattern that distinguishes old and new buckets
  size_t split_bit = 1ULL << local_depth;

  // Update directory pointers
  for (size_t i = 0; i < directory_.size(); ++i) {
    if (directory_[i] == old_bucket) {
      // Check if this index should point to new bucket
      if ((i & split_bit) != 0) {
        directory_[i] = new_bucket;
      }
    }
  }

  // Redistribute entries
  auto &entries = old_bucket->entries();
  std::vector<std::pair<KeyType, ValueType>> to_keep;
  std::vector<std::pair<KeyType, ValueType>> to_move;

  for (const auto &entry : entries) {
    size_t hash = std::hash<KeyType>{}(entry.first);
    if ((hash & split_bit) != 0) {
      to_move.push_back(entry);
    } else {
      to_keep.push_back(entry);
    }
  }

  entries = std::move(to_keep);
  for (const auto &entry : to_move) {
    new_bucket->insert(entry.first, entry.second);
  }
}

template <typename KeyType, typename ValueType>
void HashIndex<KeyType, ValueType>::grow_directory() {
  size_t old_size = directory_.size();
  directory_.resize(old_size * 2);

  // Copy bucket pointers
  for (size_t i = 0; i < old_size; ++i) {
    directory_[i + old_size] = directory_[i];
  }

  ++global_depth_;
}

} // namespace entropy
