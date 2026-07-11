/**
 * @file transaction_manager.cpp
 * @brief Transaction Manager implementation
 */

#include "transaction/transaction_manager.hpp"

#include <algorithm>
#include <shared_mutex>
#include <unordered_set>

#include "common/logger.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/page.hpp"
#include "storage/table_heap.hpp"
#include "storage/tuple.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/log_record.hpp"
#include "transaction/mvcc.hpp"
#include "transaction/version_store.hpp"
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

    // Draw the snapshot from the single logical clock so start_ts and every
    // commit_ts come from one monotonic source; otherwise begin_ts <= start_ts
    // would always hold (steady_clock vs. a small logical counter) and SI would
    // collapse to read-latest. Without an MVCC manager the constructor default
    // stands in (direct-construction unit tests).
    if (mvcc_) {
        txn->set_start_ts(mvcc_->get_timestamp());
    }

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

    // Commit timestamp comes from the single logical clock so it is
    // well-ordered against every snapshot start_ts. Without an MVCC manager
    // wired (legacy in-memory setups with no version store) there is no
    // visibility to order, so the transaction's own start stamp stands in.
    const uint64_t commit_ts = mvcc_ ? mvcc_->get_timestamp() : txn->start_ts();
    txn->set_commit_ts(commit_ts);

    // Stamp this transaction's version timestamps (begin_ts/end_ts) so readers
    // with a later snapshot observe the new value and earlier snapshots keep
    // reading the retained before-image.
    if (version_store_) {
        version_store_->finalize(txn, commit_ts);
    }

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

    // Garbage-collect version chains no remaining snapshot can observe. The
    // bound is the oldest active snapshot, or the current clock value when
    // idle (every retired version is then unreachable). Versions retired by
    // THIS commit carry timestamps newer than any currently active snapshot,
    // so while the bound is pinned by an old transaction a re-run collects
    // nothing — skip the store-wide walk until the bound actually advances.
    // Runs under mutex_; the store takes its own latch, and nothing the
    // store calls ever takes mutex_, so the order is acyclic.
    if (version_store_ && mvcc_) {
        uint64_t min_active_start_ts = mvcc_->current_timestamp();
        for (const auto& [id, active] : txn_map_) {
            min_active_start_ts =
                std::min(min_active_start_ts, active->start_ts());
        }
        if (min_active_start_ts > last_gc_bound_) {
            last_gc_bound_ = min_active_start_ts;
            version_store_->gc(min_active_start_ts);
        }
    }
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

    // Undo all modifications in reverse order using the write set, collecting
    // the pages the undo compensated so they can be forced to disk before the
    // WAL ABORT record is made durable.
    std::unordered_set<page_id_t> undone_pages;
    const auto& write_set = txn->write_set();
    for (auto it = write_set.rbegin(); it != write_set.rend(); ++it) {
        undo_write_record(*it);
        if (it->rid.page_id >= 0) {
            undone_pages.insert(it->rid.page_id);
        }
    }

    // Durability ordering (mirrors RecoveryManager::undo_phase): every page the
    // undo touched must reach disk BEFORE the ABORT record becomes durable. A
    // durable ABORT paired with unflushed compensation is a data hazard — after
    // a crash, recovery treats the transaction as already rolled back and never
    // re-runs its undo, so the uncommitted mutation (possibly already stolen to
    // disk) would be stranded there permanently.
    bool compensation_durable = true;
    if (buffer_pool_ != nullptr) {
        for (page_id_t page_id : undone_pages) {
            // fetch_page pins the page (re-reading from disk if it was evicted,
            // in which case its state is already durable), so a false from
            // flush_page can only mean a real write failure.
            Page* page = buffer_pool_->fetch_page(page_id);
            if (page == nullptr) {
                compensation_durable = false;
                continue;
            }
            const bool flushed = buffer_pool_->flush_page(page_id);
            buffer_pool_->unpin_page(page_id, false);
            if (!flushed) {
                compensation_durable = false;
            }
        }
    }

    // Log ABORT record only once its compensation is durable. If a flush
    // failed, withhold the durable ABORT so a later recovery still treats this
    // transaction as a loser and re-runs its (idempotent, state-checked) undo.
    if (wal_manager_ && compensation_durable) {
        LogRecord abort_record = LogRecord::make_abort(txn->txn_id(), txn->prev_lsn());
        lsn_t lsn = wal_manager_->append_log(abort_record);
        txn->set_prev_lsn(lsn);

        // Flush abort record: this is the durability point the compensation
        // above is guaranteed to precede on disk.
        (void)wal_manager_->flush_to_lsn(lsn);
    } else if (wal_manager_) {
        LOG_ERROR("Abort of transaction {} could not flush undo compensation; "
                  "withholding durable ABORT so recovery re-runs the undo",
                  txn->txn_id());
    }

    // Drop this transaction's uncommitted MVCC versions: creations are removed
    // from their chains and uncommitted deletes reverted so the underlying
    // committed version survives.
    if (version_store_) {
        version_store_->rollback(txn);
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
            // State-checked: a still-occupied slot means the physical delete
            // never ran (e.g. a relocation that failed between its version
            // conversion and the slot free) — nothing to restore.
            Tuple existing;
            if (heap->get_tuple(record.rid, &existing).ok()) {
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

void TransactionManager::stamp_page_lsn(RID rid, lsn_t lsn) {
    if (buffer_pool_ == nullptr || lsn == INVALID_LSN || rid.page_id < 0) {
        return;
    }
    // Record the page's highest applied LSN so the buffer pool's WAL flush hook
    // can enforce WAL-before-page and recovery can gate idempotent redo. The
    // executor mutated the page's bytes just before this call, so it is already
    // dirty; marking it dirty again on unpin is harmless.
    Page* page = buffer_pool_->fetch_page(rid.page_id);
    if (page == nullptr) {
        return;
    }
    if (lsn > page->lsn()) {
        page->set_lsn(lsn);
    }
    buffer_pool_->unpin_page(rid.page_id, true);
}

lsn_t TransactionManager::log_insert(Transaction* txn, oid_t table_oid, RID rid,
                                     const std::vector<char>& tuple_data) {
    if (txn == nullptr) {
        return INVALID_LSN;
    }

    // Hold the checkpoint barrier (shared) across append + page-LSN stamp so a
    // concurrent checkpoint cannot capture its redo anchor between the two.
    std::shared_lock<std::shared_mutex> barrier(checkpoint_latch_);

    lsn_t lsn = INVALID_LSN;
    if (wal_manager_) {
        LogRecord record =
            LogRecord::make_insert(txn->txn_id(), txn->prev_lsn(), table_oid, rid, tuple_data);
        lsn = wal_manager_->append_log(record);
        txn->set_prev_lsn(lsn);
        stamp_page_lsn(rid, lsn);
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

    std::shared_lock<std::shared_mutex> barrier(checkpoint_latch_);

    lsn_t lsn = INVALID_LSN;
    if (wal_manager_) {
        LogRecord record =
            LogRecord::make_delete(txn->txn_id(), txn->prev_lsn(), table_oid, rid, tuple_data);
        lsn = wal_manager_->append_log(record);
        txn->set_prev_lsn(lsn);
        stamp_page_lsn(rid, lsn);
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

    std::shared_lock<std::shared_mutex> barrier(checkpoint_latch_);

    lsn_t lsn = INVALID_LSN;
    if (wal_manager_) {
        LogRecord record = LogRecord::make_update(txn->txn_id(), txn->prev_lsn(), table_oid, rid,
                                                  old_data, new_data);
        lsn = wal_manager_->append_log(record);
        txn->set_prev_lsn(lsn);
        stamp_page_lsn(rid, lsn);
    }

    // Always track write set with old data for abort undo
    txn->add_write_record(WriteRecord(WriteType::UPDATE, table_oid, rid, old_data));

    return lsn;
}

}  // namespace entropy
