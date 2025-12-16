/**
 * @file transaction_manager.cpp
 * @brief Transaction Manager implementation
 */

#include "transaction/transaction_manager.hpp"

#include <chrono>

#include "common/logger.hpp"
#include "transaction/log_record.hpp"
#include "transaction/wal.hpp"

namespace entropy {

TransactionManager::TransactionManager() : wal_manager_(nullptr) {}

TransactionManager::TransactionManager(std::shared_ptr<WALManager> wal_manager)
    : wal_manager_(std::move(wal_manager)) {}

Transaction* TransactionManager::begin(IsolationLevel isolation) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Create new transaction
    txn_id_t txn_id = next_txn_id_++;
    auto txn = std::make_unique<Transaction>(txn_id, isolation);

    // Log BEGIN record
    if (wal_manager_) {
        LogRecord begin_record = LogRecord::make_begin(txn_id);
        lsn_t lsn = wal_manager_->append_log(begin_record);
        txn->set_prev_lsn(lsn);
    }

    Transaction* ptr = txn.get();
    txn_map_[txn_id] = std::move(txn);

    LOG_DEBUG("Transaction {} started", txn_id);
    return ptr;
}

void TransactionManager::commit(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (txn->state() != TransactionState::GROWING &&
        txn->state() != TransactionState::SHRINKING) {
        LOG_WARN("Attempting to commit transaction {} in state {}", txn->txn_id(),
                 transaction_state_to_string(txn->state()));
        return;
    }

    // Log COMMIT record
    if (wal_manager_) {
        LogRecord commit_record = LogRecord::make_commit(txn->txn_id(), txn->prev_lsn());
        lsn_t lsn = wal_manager_->append_log(commit_record);
        txn->set_prev_lsn(lsn);

        // Force flush on commit - this ensures durability
        Status status = wal_manager_->flush_to_lsn(lsn);
        if (!status.ok()) {
            LOG_ERROR("Failed to flush WAL on commit: {}", status.message());
            // In a production system, we might want to handle this more gracefully
        }
    }

    // Set commit timestamp
    txn->set_commit_ts(static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count()));

    // Update state
    txn->set_state(TransactionState::COMMITTED);

    // Clear write set (no longer needed)
    txn->clear_write_set();

    LOG_DEBUG("Transaction {} committed", txn->txn_id());

    // Remove from active transactions
    txn_map_.erase(txn->txn_id());
}

void TransactionManager::abort(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (txn->state() != TransactionState::GROWING &&
        txn->state() != TransactionState::SHRINKING) {
        LOG_WARN("Attempting to abort transaction {} in state {}", txn->txn_id(),
                 transaction_state_to_string(txn->state()));
        return;
    }

    // TODO: Undo all modifications in reverse order using write set
    // For now, we just mark as aborted
    // The recovery manager will handle actual undo

    // Log ABORT record
    if (wal_manager_) {
        LogRecord abort_record = LogRecord::make_abort(txn->txn_id(), txn->prev_lsn());
        lsn_t lsn = wal_manager_->append_log(abort_record);
        txn->set_prev_lsn(lsn);

        // Flush abort record
        (void)wal_manager_->flush_to_lsn(lsn);
    }

    // Update state
    txn->set_state(TransactionState::ABORTED);

    LOG_DEBUG("Transaction {} aborted", txn->txn_id());

    // Remove from active transactions
    txn_map_.erase(txn->txn_id());
}

Transaction* TransactionManager::get_transaction(txn_id_t txn_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = txn_map_.find(txn_id);
    return (it != txn_map_.end()) ? it->second.get() : nullptr;
}

std::vector<txn_id_t> TransactionManager::get_active_txn_ids() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<txn_id_t> ids;
    ids.reserve(txn_map_.size());
    for (const auto& [id, txn] : txn_map_) {
        ids.push_back(id);
    }
    return ids;
}

size_t TransactionManager::active_transaction_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return txn_map_.size();
}

lsn_t TransactionManager::log_insert(Transaction* txn, oid_t table_oid, RID rid,
                                     const std::vector<char>& tuple_data) {
    if (!wal_manager_ || txn == nullptr) {
        return INVALID_LSN;
    }

    LogRecord record =
        LogRecord::make_insert(txn->txn_id(), txn->prev_lsn(), table_oid, rid, tuple_data);
    lsn_t lsn = wal_manager_->append_log(record);
    txn->set_prev_lsn(lsn);

    // Add to write set for potential rollback
    txn->add_write_record(WriteRecord(WriteType::INSERT, table_oid, rid));

    return lsn;
}

lsn_t TransactionManager::log_delete(Transaction* txn, oid_t table_oid, RID rid,
                                     const std::vector<char>& tuple_data) {
    if (!wal_manager_ || txn == nullptr) {
        return INVALID_LSN;
    }

    LogRecord record =
        LogRecord::make_delete(txn->txn_id(), txn->prev_lsn(), table_oid, rid, tuple_data);
    lsn_t lsn = wal_manager_->append_log(record);
    txn->set_prev_lsn(lsn);

    // Add to write set with old data for rollback
    txn->add_write_record(WriteRecord(WriteType::DELETE, table_oid, rid, tuple_data));

    return lsn;
}

lsn_t TransactionManager::log_update(Transaction* txn, oid_t table_oid, RID rid,
                                     const std::vector<char>& old_data,
                                     const std::vector<char>& new_data) {
    if (!wal_manager_ || txn == nullptr) {
        return INVALID_LSN;
    }

    LogRecord record =
        LogRecord::make_update(txn->txn_id(), txn->prev_lsn(), table_oid, rid, old_data, new_data);
    lsn_t lsn = wal_manager_->append_log(record);
    txn->set_prev_lsn(lsn);

    // Add to write set with old data for rollback
    txn->add_write_record(WriteRecord(WriteType::UPDATE, table_oid, rid, old_data));

    return lsn;
}

}  // namespace entropy
