#pragma once

/**
 * @file table_heap.hpp
 * @brief Table storage using heap file organization
 */

#include <memory>

#include "common/types.hpp"
#include "entropy/status.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/tuple.hpp"

namespace entropy {

/**
 * @brief Table Heap - stores tuples in a heap file
 */
class TableHeap {
public:
    explicit TableHeap(std::shared_ptr<BufferPoolManager> buffer_pool);
    ~TableHeap() = default;

    /// Insert a tuple into the table
    [[nodiscard]] Status insert_tuple(const Tuple& tuple, RID* rid);

    /// Delete a tuple from the table
    [[nodiscard]] Status delete_tuple(const RID& rid);

    /// Update a tuple in the table
    [[nodiscard]] Status update_tuple(const Tuple& tuple, const RID& rid);

    /// Get a tuple by RID
    [[nodiscard]] Status get_tuple(const RID& rid, Tuple* tuple);

private:
    std::shared_ptr<BufferPoolManager> buffer_pool_;
    page_id_t first_page_id_ = INVALID_PAGE_ID;
};

}  // namespace entropy
