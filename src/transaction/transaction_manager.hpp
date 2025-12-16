#pragma once

/**
 * @file transaction_manager.hpp
 * @brief Transaction lifecycle management
 */

#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/types.hpp"
#include "transaction/transaction.hpp"

namespace entropy {

class TransactionManager {
public:
    TransactionManager() = default;

    [[nodiscard]] Transaction* begin();
    void commit(Transaction* txn);
    void abort(Transaction* txn);

private:
    std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> txn_map_;
    txn_id_t next_txn_id_ = 1;
    std::mutex mutex_;
};

}  // namespace entropy
