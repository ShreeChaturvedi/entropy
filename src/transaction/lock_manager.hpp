#pragma once

/**
 * @file lock_manager.hpp
 * @brief Lock management for 2PL
 */

#include "common/types.hpp"
#include "entropy/status.hpp"

namespace entropy {

class Transaction;

enum class LockMode { SHARED, EXCLUSIVE };

class LockManager {
public:
    LockManager() = default;

    [[nodiscard]] Status lock_table(Transaction* txn, oid_t table_oid, LockMode mode);
    [[nodiscard]] Status lock_row(Transaction* txn, oid_t table_oid, const RID& rid, LockMode mode);
    [[nodiscard]] Status unlock_table(Transaction* txn, oid_t table_oid);
    [[nodiscard]] Status unlock_row(Transaction* txn, oid_t table_oid, const RID& rid);
};

}  // namespace entropy
