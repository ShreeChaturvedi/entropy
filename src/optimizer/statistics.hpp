#pragma once

/**
 * @file statistics.hpp
 * @brief Table and column statistics
 */

#include <string>

#include "common/types.hpp"

namespace entropy {

class Statistics {
public:
    Statistics() = default;

    [[nodiscard]] size_t table_cardinality(oid_t table_oid) const;
    [[nodiscard]] double column_selectivity(oid_t table_oid,
                                            column_id_t column_id) const;
};

}  // namespace entropy
