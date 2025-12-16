/**
 * @file lock_manager.cpp
 * @brief Lock Manager stub
 */

#include "transaction/lock_manager.hpp"

namespace entropy {

Status LockManager::lock_table([[maybe_unused]] Transaction* txn,
                                [[maybe_unused]] oid_t table_oid,
                                [[maybe_unused]] LockMode mode) {
    return Status::Ok();  // Stub
}

Status LockManager::lock_row([[maybe_unused]] Transaction* txn,
                              [[maybe_unused]] oid_t table_oid,
                              [[maybe_unused]] const RID& rid,
                              [[maybe_unused]] LockMode mode) {
    return Status::Ok();  // Stub
}

Status LockManager::unlock_table([[maybe_unused]] Transaction* txn,
                                  [[maybe_unused]] oid_t table_oid) {
    return Status::Ok();  // Stub
}

Status LockManager::unlock_row([[maybe_unused]] Transaction* txn,
                                [[maybe_unused]] oid_t table_oid,
                                [[maybe_unused]] const RID& rid) {
    return Status::Ok();  // Stub
}

}  // namespace entropy
