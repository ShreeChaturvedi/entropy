/**
 * @file recovery.cpp
 * @brief Recovery Manager stub
 */

#include "transaction/recovery.hpp"

namespace entropy {

RecoveryManager::RecoveryManager(std::shared_ptr<BufferPoolManager> buffer_pool,
                                 std::shared_ptr<WALManager> wal)
    : buffer_pool_(std::move(buffer_pool)), wal_(std::move(wal)) {}

Status RecoveryManager::recover() {
    // TODO: Implement ARIES recovery
    return Status::Ok();
}

}  // namespace entropy
