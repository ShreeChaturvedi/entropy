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

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "common/types.hpp"
#include "transaction/transaction.hpp"

namespace entropy {

// Forward declarations
class WALManager;

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

private:
    std::shared_ptr<WALManager> wal_manager_;
    std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> txn_map_;
    txn_id_t next_txn_id_ = 1;
    mutable std::mutex mutex_;
};

}  // namespace entropy
