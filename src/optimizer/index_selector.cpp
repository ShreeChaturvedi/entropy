/**
 * @file index_selector.cpp
 * @brief Index selector stub
 */

#include "optimizer/index_selector.hpp"

namespace entropy {

IndexSelector::IndexSelector(std::shared_ptr<Catalog> catalog)
    : catalog_(std::move(catalog)) {}

oid_t IndexSelector::select_index([[maybe_unused]] oid_t table_oid,
                                   [[maybe_unused]] column_id_t column_id) const {
    return INVALID_OID;  // Stub - no index selected
}

}  // namespace entropy
