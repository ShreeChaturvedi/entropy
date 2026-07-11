#pragma once

/**
 * @file transaction_manager.hpp
 * @brief Transaction lifecycle management
 *
 * The TransactionManager is responsible for:
 * 1. Creating and tracking active transactions
 * 2. Coordinating commit and abort operations
 * 3. Integrating with WAL for durability
 * 4. Managing transaction IDs
 */

#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "common/types.hpp"
#include "transaction/transaction.hpp"

namespace entropy {

// Forward declarations
class WALManager;
class LockManager;
class TableHeap;
class MVCCManager;
class VersionStore;
class BufferPoolManager;

/**
 * @brief Manages transaction lifecycle
 *
 * Thread safety: All public methods are thread-safe.
 */
class TransactionManager {
public:
    /**
     * @brief Create a transaction manager without WAL
     * (useful for testing or in-memory operations)
     */
    TransactionManager();

    /**
     * @brief Create a transaction manager with WAL support
     * @param wal_manager The WAL manager for logging
     */
    explicit TransactionManager(std::shared_ptr<WALManager> wal_manager);

    ~TransactionManager() = default;

    // Non-copyable
    TransactionManager(const TransactionManager&) = delete;
    TransactionManager& operator=(const TransactionManager&) = delete;

    // ─────────────────────────────────────────────────────────────────────────
    // Transaction Operations
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Begin a new transaction
     * @param isolation Isolation level for the transaction
     * @return Pointer to the new transaction (owned by TransactionManager)
     *
     * Writes a BEGIN log record to WAL.
     */
    [[nodiscard]] Transaction* begin(IsolationLevel isolation = IsolationLevel::REPEATABLE_READ);

    /**
     * @brief Commit a transaction
     * @param txn The transaction to commit
     *
     * This method:
     * 1. Writes a COMMIT log record
     * 2. Flushes the WAL to disk (force on commit)
     * 3. Releases locks
     * 4. Removes transaction from active set
     */
    void commit(Transaction* txn);

    /**
     * @brief Abort a transaction
     * @param txn The transaction to abort
     *
     * This method:
     * 1. Undoes all modifications in reverse order
     * 2. Writes an ABORT log record
     * 3. Releases locks
     * 4. Removes transaction from active set
     */
    void abort(Transaction* txn);

    // ─────────────────────────────────────────────────────────────────────────
    // Transaction Query
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Get a transaction by ID
     * @return Pointer to transaction or nullptr if not found
     */
    [[nodiscard]] Transaction* get_transaction(txn_id_t txn_id);

    /**
     * @brief Is @p txn_id a live transaction still identified by @p txn?
     *
     * True only when the active set holds @p txn_id AND it still maps to the
     * same Transaction object. A session validates a cached thread->transaction
     * binding through this before reusing it: once its transaction has
     * committed or aborted (erased from the active set) the check fails, so a
     * recycled thread id that inherited a stale binding starts a fresh
     * transaction instead of misbinding to a dead one. Compares the pointer
     * without dereferencing it, so it is safe to call with a possibly-stale
     * binding.
     */
    [[nodiscard]] bool is_active_transaction(txn_id_t txn_id,
                                             const Transaction* txn) const;

    /**
     * @brief Get all active transaction IDs
     */
    [[nodiscard]] std::vector<txn_id_t> get_active_txn_ids() const;

    /**
     * @brief Get the number of active transactions
     */
    [[nodiscard]] size_t active_transaction_count() const;

    // ─────────────────────────────────────────────────────────────────────────
    // WAL Integration
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Log an INSERT operation
     */
    lsn_t log_insert(Transaction* txn, oid_t table_oid, RID rid,
                     const std::vector<char>& tuple_data);

    /**
     * @brief Log a DELETE operation
     */
    lsn_t log_delete(Transaction* txn, oid_t table_oid, RID rid,
                     const std::vector<char>& tuple_data);

    /**
     * @brief Log an UPDATE operation
     */
    lsn_t log_update(Transaction* txn, oid_t table_oid, RID rid,
                     const std::vector<char>& old_data, const std::vector<char>& new_data);

    /**
     * @brief Get the WAL manager (for recovery)
     */
    [[nodiscard]] WALManager* wal_manager() const noexcept { return wal_manager_.get(); }

    /**
     * @brief Attach a lock manager so commit/abort release locks
     * @note Not owned; must outlive this TransactionManager
     */
    void set_lock_manager(LockManager* lock_manager) noexcept {
        lock_manager_ = lock_manager;
    }

    /**
     * @brief Attach the MVCC policy object (single logical clock source)
     *
     * Once set, begin() draws each transaction's start_ts (snapshot) and
     * commit() draws each commit_ts from MVCCManager::get_timestamp(), so all
     * MVCC visibility comparisons are well-ordered against one clock.
     */
    void set_mvcc(std::shared_ptr<MVCCManager> mvcc) noexcept {
        mvcc_ = std::move(mvcc);
    }

    /**
     * @brief Attach the in-memory version store
     *
     * commit() finalizes this transaction's version timestamps and abort()
     * rolls back its uncommitted versions through the store.
     */
    void set_version_store(std::shared_ptr<VersionStore> version_store) noexcept {
        version_store_ = std::move(version_store);
    }

    /**
     * @brief Attach a buffer pool so abort() can flush its undo compensation
     *
     * abort() flushes every page mutated by its write-set undo before it makes
     * the WAL ABORT record durable (see abort()). Not owned; must outlive this
     * TransactionManager.
     */
    void set_buffer_pool(BufferPoolManager* buffer_pool) noexcept {
        buffer_pool_ = buffer_pool;
    }

    /**
     * @brief Seed the next transaction id from a recovered high-water mark
     *
     * Called on startup with RecoveryManager::next_txn_id() so post-restart
     * transactions never alias ids recovered from the WAL (#19). Only advances
     * the counter; never rewinds it.
     */
    void seed_next_txn_id(txn_id_t next) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (next > next_txn_id_) {
            next_txn_id_ = next;
        }
    }

    /**
     * @brief Resolve table OID -> TableHeap for abort undo
     *
     * Called for each write-set entry during abort. Returning nullptr skips
     * physical undo for that entry (logged as a warning).
     */
    using TableResolver = std::function<TableHeap*(oid_t table_oid)>;
    void set_table_resolver(TableResolver resolver) {
        table_resolver_ = std::move(resolver);
    }

    /**
     * @brief Barrier that quiesces heap writers against a checkpoint
     *
     * The heap write path (TableHeap::insert_tuple/delete_tuple/
     * update_tuple_in_place) holds this SHARED across its ENTIRE critical
     * section — the page mutation plus the logging hook that appends the WAL
     * record and stamps the page LSN. A checkpoint takes it EXCLUSIVELY around
     * its redo-anchor capture + page flush (see
     * RecoveryManager::create_checkpoint), so it can never flush a mutated page
     * whose record is not yet appended and stamped (crash-safety F3). The
     * barrier is acquired AFTER the heap's own lock (heap-lock → barrier), and
     * row locks + first-updater-wins run BEFORE the mutation, so a writer never
     * blocks on another transaction while holding this shared latch and the
     * checkpoint's exclusive wait is bounded.
     */
    [[nodiscard]] std::shared_mutex& checkpoint_barrier() noexcept {
        return checkpoint_latch_;
    }

private:
    /**
     * @brief Undo one write-set entry (inverse of the logged operation)
     */
    void undo_write_record(const WriteRecord& record);

    /**
     * @brief Append a redoable compensation record for an undone write-set entry
     *
     * Called for each entry after undo_write_record during abort(). Logs the
     * inverse operation (DELETE for an undone INSERT, INSERT of the before-image
     * for an undone DELETE, UPDATE back to the before-image for an undone UPDATE)
     * as an ordinary data record and stamps its LSN onto the page. Recovery's
     * repeat-history redo then reproduces the rollback of a transaction that
     * aborted during normal operation even when its compensated pages were lost
     * at a crash — the undo phase never revisits it once its ABORT is durable
     * (#75/#81). No-op without a WAL manager.
     */
    void log_compensation(Transaction* txn, const WriteRecord& record);

    /**
     * @brief Stamp @p lsn onto the page holding @p rid (WAL-before-page / redo)
     *
     * The page LSN records the highest log record whose effect the page
     * reflects, which the buffer pool's WAL flush hook reads to enforce
     * WAL-before-page and recovery reads to gate idempotent redo. No-op without
     * a buffer pool, an invalid LSN, or an invalid page id.
     */
    void stamp_page_lsn(RID rid, lsn_t lsn);

    std::shared_ptr<WALManager> wal_manager_;
    std::shared_ptr<MVCCManager> mvcc_;
    std::shared_ptr<VersionStore> version_store_;
    LockManager* lock_manager_ = nullptr;
    BufferPoolManager* buffer_pool_ = nullptr;
    TableResolver table_resolver_;
    std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> txn_map_;
    txn_id_t next_txn_id_ = 1;
    /// Highest version-GC bound already applied (guarded by mutex_). A commit
    /// skips the store-wide GC walk while the oldest active snapshot pins the
    /// bound, because nothing new can become collectible until it advances.
    uint64_t last_gc_bound_ = 0;
    mutable std::mutex mutex_;
    // Quiesces heap writers against a concurrent checkpoint (see
    // checkpoint_barrier()). This class only stores and hands the latch out:
    // TableHeap's write paths hold it SHARED across mutate + log, and
    // create_checkpoint takes it EXCLUSIVELY. It never nests with mutex_.
    std::shared_mutex checkpoint_latch_;
};

}  // namespace entropy
