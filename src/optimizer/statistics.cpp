/**
 * @file statistics.cpp
 * @brief Statistics stub
 */

#include "optimizer/statistics.hpp"

namespace entropy {

size_t Statistics::table_cardinality([[maybe_unused]] oid_t table_oid) const {
    return 1000;  // Stub
}

double Statistics::column_selectivity([[maybe_unused]] oid_t table_oid,
                                       [[maybe_unused]] column_id_t column_id) const {
    return 0.1;  // Stub
}

}  // namespace entropy
