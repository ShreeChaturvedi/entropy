/**
 * @file hash_index.cpp
 * @brief Hash Index implementation stub
 */

#include "storage/hash_index.hpp"

namespace entropy {

template class HashIndex<int32_t, RID>;
template class HashIndex<int64_t, RID>;

template <typename KeyType, typename ValueType>
HashIndex<KeyType, ValueType>::HashIndex(
    std::shared_ptr<BufferPoolManager> buffer_pool)
    : buffer_pool_(std::move(buffer_pool)) {}

template <typename KeyType, typename ValueType>
Status HashIndex<KeyType, ValueType>::insert(
    [[maybe_unused]] const KeyType& key,
    [[maybe_unused]] const ValueType& value) {
    return Status::NotSupported("HashIndex::insert not implemented");
}

template <typename KeyType, typename ValueType>
Status HashIndex<KeyType, ValueType>::remove(
    [[maybe_unused]] const KeyType& key) {
    return Status::NotSupported("HashIndex::remove not implemented");
}

template <typename KeyType, typename ValueType>
std::optional<ValueType> HashIndex<KeyType, ValueType>::find(
    [[maybe_unused]] const KeyType& key) {
    return std::nullopt;
}

}  // namespace entropy
