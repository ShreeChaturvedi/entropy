/**
 * @file transaction_manager.cpp
 * @brief Transaction Manager implementation
 */

#include "transaction/transaction_manager.hpp"

namespace entropy {

Transaction* TransactionManager::begin() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto txn = std::make_unique<Transaction>(next_txn_id_++);
    auto* ptr = txn.get();
    txn_map_[ptr->txn_id()] = std::move(txn);
    return ptr;
}

void TransactionManager::commit(Transaction* txn) {
    std::lock_guard<std::mutex> lock(mutex_);
    txn->set_state(TransactionState::COMMITTED);
    txn_map_.erase(txn->txn_id());
}

void TransactionManager::abort(Transaction* txn) {
    std::lock_guard<std::mutex> lock(mutex_);
    txn->set_state(TransactionState::ABORTED);
    txn_map_.erase(txn->txn_id());
}

}  // namespace entropy
