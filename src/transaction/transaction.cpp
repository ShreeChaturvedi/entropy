/**
 * @file transaction.cpp
 * @brief Transaction implementation
 */

#include "transaction/transaction.hpp"

#include <chrono>

namespace entropy {

Transaction::Transaction(txn_id_t txn_id, IsolationLevel isolation)
    : txn_id_(txn_id)
    , isolation_level_(isolation)
    , start_ts_(static_cast<uint64_t>(
          std::chrono::steady_clock::now().time_since_epoch().count())) {}

}  // namespace entropy
