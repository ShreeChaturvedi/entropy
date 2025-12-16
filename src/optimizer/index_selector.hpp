#pragma once

/**
 * @file index_selector.hpp
 * @brief Index selection for query optimization
 */

#include <memory>

#include "common/types.hpp"

namespace entropy {

class Catalog;

class IndexSelector {
public:
    explicit IndexSelector(std::shared_ptr<Catalog> catalog);

    [[nodiscard]] oid_t select_index(oid_t table_oid, column_id_t column_id) const;

private:
    std::shared_ptr<Catalog> catalog_;
};

}  // namespace entropy
