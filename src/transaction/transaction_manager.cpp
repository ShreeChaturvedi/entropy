/**
 * @file transaction_manager.cpp
 * @brief Transaction Manager implementation
 */

#include "transaction/transaction_manager.hpp"

#include <chrono>

#include "common/logger.hpp"
#include "storage/table_heap.hpp"
#include "storage/tuple.hpp"
#include "transaction/lock_manager.hpp"
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

    // Release locks before dropping the transaction
    if (lock_manager_ != nullptr) {
        lock_manager_->release_all_locks(txn);
    }

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

    // A deadlock victim is already marked ABORTED by the lock manager (so its
    // waiter loops terminate), but nothing has been finalized: its writes must
    // still be undone, the WAL ABORT record appended, and the transaction
    // removed from the active set. Let it through; refuse everything else that
    // is not active.
    const TransactionState state = txn->state();
    const bool deadlock_victim_pending =
        state == TransactionState::ABORTED && txn->aborted_by_deadlock();
    if (state != TransactionState::GROWING &&
        state != TransactionState::SHRINKING && !deadlock_victim_pending) {
        LOG_WARN("Attempting to abort transaction {} in state {}", txn->txn_id(),
                 transaction_state_to_string(state));
        return;
    }

    // Consume the mark: finalization runs exactly once. A stray second abort()
    // on a still-live victim object then sees ABORTED without the mark and is
    // a no-op instead of appending a duplicate WAL ABORT record.
    txn->clear_aborted_by_deadlock();

    // Undo all modifications in reverse order using the write set
    const auto& write_set = txn->write_set();
    for (auto it = write_set.rbegin(); it != write_set.rend(); ++it) {
        undo_write_record(*it);
    }

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

    // Release locks after undo so concurrent writers stay blocked during
    // rollback. Deadlock victims keep their granted locks through the lock
    // manager's wait-loop exit precisely so this ordering holds for them too.
    if (lock_manager_ != nullptr) {
        lock_manager_->release_all_locks(txn);
    }

    txn->clear_write_set();

    LOG_DEBUG("Transaction {} aborted", txn->txn_id());

    // Remove from active transactions
    txn_map_.erase(txn->txn_id());
}

void TransactionManager::undo_write_record(const WriteRecord& record) {
    if (!table_resolver_) {
        LOG_WARN("Abort undo skipped for table {}: no table resolver configured",
                 record.table_oid);
        return;
    }

    TableHeap* heap = table_resolver_(record.table_oid);
    if (heap == nullptr) {
        LOG_WARN("Abort undo skipped: table {} not found", record.table_oid);
        return;
    }

    switch (record.type) {
        case WriteType::INSERT: {
            // Undo INSERT = delete the inserted tuple
            Status status = heap->delete_tuple(record.rid);
            if (!status.ok()) {
                LOG_WARN("Abort undo INSERT failed at ({},{}): {}", record.rid.page_id,
                         record.rid.slot_id, status.message());
            }
            break;
        }
        case WriteType::DELETE: {
            // Undo DELETE = restore old tuple data at the same RID when possible
            if (record.old_data.empty()) {
                LOG_WARN("Abort undo DELETE missing old_data for ({},{})",
                         record.rid.page_id, record.rid.slot_id);
                break;
            }
            Tuple restored(record.old_data, record.rid);
            Status status = heap->restore_tuple(record.rid, restored);
            if (!status.ok()) {
                LOG_WARN("Abort undo DELETE failed at ({},{}): {}", record.rid.page_id,
                         record.rid.slot_id, status.message());
            }
            break;
        }
        case WriteType::UPDATE: {
            // Undo UPDATE = restore old tuple data
            if (record.old_data.empty()) {
                LOG_WARN("Abort undo UPDATE missing old_data for ({},{})",
                         record.rid.page_id, record.rid.slot_id);
                break;
            }
            Tuple old_tuple(record.old_data, record.rid);
            Status status = heap->update_tuple(old_tuple, record.rid);
            if (!status.ok()) {
                LOG_WARN("Abort undo UPDATE failed at ({},{}): {}", record.rid.page_id,
                         record.rid.slot_id, status.message());
            }
            break;
        }
    }
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
    if (txn == nullptr) {
        return INVALID_LSN;
    }

    lsn_t lsn = INVALID_LSN;
    if (wal_manager_) {
        LogRecord record =
            LogRecord::make_insert(txn->txn_id(), txn->prev_lsn(), table_oid, rid, tuple_data);
        lsn = wal_manager_->append_log(record);
        txn->set_prev_lsn(lsn);
    }

    // Always track write set for abort undo (even without WAL)
    txn->add_write_record(WriteRecord(WriteType::INSERT, table_oid, rid));

    return lsn;
}

lsn_t TransactionManager::log_delete(Transaction* txn, oid_t table_oid, RID rid,
                                     const std::vector<char>& tuple_data) {
    if (txn == nullptr) {
        return INVALID_LSN;
    }

    lsn_t lsn = INVALID_LSN;
    if (wal_manager_) {
        LogRecord record =
            LogRecord::make_delete(txn->txn_id(), txn->prev_lsn(), table_oid, rid, tuple_data);
        lsn = wal_manager_->append_log(record);
        txn->set_prev_lsn(lsn);
    }

    // Always track write set with old data for abort undo
    txn->add_write_record(WriteRecord(WriteType::DELETE, table_oid, rid, tuple_data));

    return lsn;
}

lsn_t TransactionManager::log_update(Transaction* txn, oid_t table_oid, RID rid,
                                     const std::vector<char>& old_data,
                                     const std::vector<char>& new_data) {
    if (txn == nullptr) {
        return INVALID_LSN;
    }

    lsn_t lsn = INVALID_LSN;
    if (wal_manager_) {
        LogRecord record = LogRecord::make_update(txn->txn_id(), txn->prev_lsn(), table_oid, rid,
                                                  old_data, new_data);
        lsn = wal_manager_->append_log(record);
        txn->set_prev_lsn(lsn);
    }

    // Always track write set with old data for abort undo
    txn->add_write_record(WriteRecord(WriteType::UPDATE, table_oid, rid, old_data));

    return lsn;
}

}  // namespace entropy
