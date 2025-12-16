/**
 * @file table_heap.cpp
 * @brief Table Heap implementation stub
 */

#include "storage/table_heap.hpp"

namespace entropy {

TableHeap::TableHeap(std::shared_ptr<BufferPoolManager> buffer_pool)
    : buffer_pool_(std::move(buffer_pool)) {}

Status TableHeap::insert_tuple([[maybe_unused]] const Tuple& tuple,
                               [[maybe_unused]] RID* rid) {
    // TODO: Implement tuple insertion
    return Status::NotSupported("TableHeap::insert_tuple not implemented");
}

Status TableHeap::delete_tuple([[maybe_unused]] const RID& rid) {
    // TODO: Implement tuple deletion
    return Status::NotSupported("TableHeap::delete_tuple not implemented");
}

Status TableHeap::update_tuple([[maybe_unused]] const Tuple& tuple,
                               [[maybe_unused]] const RID& rid) {
    // TODO: Implement tuple update
    return Status::NotSupported("TableHeap::update_tuple not implemented");
}

Status TableHeap::get_tuple([[maybe_unused]] const RID& rid,
                            [[maybe_unused]] Tuple* tuple) {
    // TODO: Implement tuple retrieval
    return Status::NotSupported("TableHeap::get_tuple not implemented");
}

}  // namespace entropy
