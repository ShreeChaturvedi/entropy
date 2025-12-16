/**
 * @file b_plus_tree.cpp
 * @brief B+ Tree implementation stub
 */

#include "storage/b_plus_tree.hpp"

namespace entropy {

// Explicit instantiation for common types
template class BPlusTree<int32_t, RID>;
template class BPlusTree<int64_t, RID>;

template <typename KeyType, typename ValueType>
BPlusTree<KeyType, ValueType>::BPlusTree(
    std::shared_ptr<BufferPoolManager> buffer_pool, page_id_t root_page_id)
    : buffer_pool_(std::move(buffer_pool)), root_page_id_(root_page_id) {}

template <typename KeyType, typename ValueType>
Status BPlusTree<KeyType, ValueType>::insert(
    [[maybe_unused]] const KeyType& key,
    [[maybe_unused]] const ValueType& value) {
    // TODO: Implement B+ tree insertion
    return Status::NotSupported("BPlusTree::insert not implemented");
}

template <typename KeyType, typename ValueType>
Status BPlusTree<KeyType, ValueType>::remove(
    [[maybe_unused]] const KeyType& key) {
    // TODO: Implement B+ tree deletion
    return Status::NotSupported("BPlusTree::remove not implemented");
}

template <typename KeyType, typename ValueType>
std::optional<ValueType> BPlusTree<KeyType, ValueType>::find(
    [[maybe_unused]] const KeyType& key) {
    // TODO: Implement B+ tree lookup
    return std::nullopt;
}

template <typename KeyType, typename ValueType>
std::vector<std::pair<KeyType, ValueType>>
BPlusTree<KeyType, ValueType>::range_scan(
    [[maybe_unused]] const KeyType& start_key,
    [[maybe_unused]] const KeyType& end_key) {
    // TODO: Implement range scan
    return {};
}

template <typename KeyType, typename ValueType>
bool BPlusTree<KeyType, ValueType>::is_empty() const {
    return root_page_id_ == INVALID_PAGE_ID;
}

}  // namespace entropy
