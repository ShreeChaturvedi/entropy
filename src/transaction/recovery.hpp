#pragma once

/**
 * @file recovery.hpp
 * @brief Crash recovery (ARIES-style)
 */

#include <memory>

#include "entropy/status.hpp"

namespace entropy {

class BufferPoolManager;
class WALManager;

class RecoveryManager {
public:
    RecoveryManager(std::shared_ptr<BufferPoolManager> buffer_pool,
                    std::shared_ptr<WALManager> wal);

    [[nodiscard]] Status recover();

private:
    std::shared_ptr<BufferPoolManager> buffer_pool_;
    std::shared_ptr<WALManager> wal_;
};

}  // namespace entropy
