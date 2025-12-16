#pragma once

/**
 * @file transaction.hpp
 * @brief Transaction object with state machine and write set tracking
 *
 * Transactions follow the 2PL (Two-Phase Locking) state machine:
 *   GROWING -> SHRINKING -> COMMITTED/ABORTED
 *
 * During GROWING phase, transaction acquires locks.
 * During SHRINKING phase, transaction releases locks (after first unlock).
 * COMMITTED/ABORTED are terminal states.
 */

#include <chrono>
#include <memory>
#include <unordered_set>
#include <vector>

#include "common/types.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Transaction State Machine
// ─────────────────────────────────────────────────────────────────────────────

enum class TransactionState : uint8_t {
    GROWING = 0,    // Acquiring locks, performing reads/writes
    SHRINKING = 1,  // Releasing locks (2PL shrinking phase)
    COMMITTED = 2,  // Successfully committed
    ABORTED = 3     // Rolled back
};

/**
 * @brief Convert transaction state to string for debugging
 */
[[nodiscard]] inline const char* transaction_state_to_string(TransactionState state) {
    switch (state) {
        case TransactionState::GROWING:
            return "GROWING";
        case TransactionState::SHRINKING:
            return "SHRINKING";
        case TransactionState::COMMITTED:
            return "COMMITTED";
        case TransactionState::ABORTED:
            return "ABORTED";
        default:
            return "UNKNOWN";
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Isolation Levels
// ─────────────────────────────────────────────────────────────────────────────

enum class IsolationLevel : uint8_t {
    READ_UNCOMMITTED = 0,  // No read locks, allows dirty reads
    READ_COMMITTED = 1,    // Read locks released immediately after read
    REPEATABLE_READ = 2,   // Read locks held until commit
    SERIALIZABLE = 3       // Read locks + predicate locks
};

// ─────────────────────────────────────────────────────────────────────────────
// Write Set Entry - tracks modifications for undo
// ─────────────────────────────────────────────────────────────────────────────

enum class WriteType : uint8_t { INSERT = 0, DELETE = 1, UPDATE = 2 };

/**
 * @brief Tracks a single write operation for undo during rollback
 */
struct WriteRecord {
    WriteType type;
    oid_t table_oid;  // Which table
    RID rid;                // Which tuple
    // For UPDATE: stores old tuple data for undo
    // For INSERT: empty (undo = delete)
    // For DELETE: stores deleted tuple data for undo
    std::vector<char> old_data;

    WriteRecord(WriteType t, oid_t tbl, RID r)
        : type(t), table_oid(tbl), rid(r) {}

    WriteRecord(WriteType t, oid_t tbl, RID r, std::vector<char> data)
        : type(t), table_oid(tbl), rid(r), old_data(std::move(data)) {}
};

// ─────────────────────────────────────────────────────────────────────────────
// Transaction Class
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Represents a database transaction
 *
 * Each transaction has:
 * - A unique transaction ID
 * - A state (GROWING, SHRINKING, COMMITTED, ABORTED)
 * - A write set for tracking modifications (used for undo on abort)
 * - Timestamps for MVCC visibility checks
 *
 * Thread safety: Individual Transaction objects are NOT thread-safe.
 * Each thread should have its own Transaction object.
 */
class Transaction {
public:
    /**
     * @brief Create a new transaction
     * @param txn_id Unique transaction identifier
     * @param isolation Isolation level (default: REPEATABLE_READ)
     */
    explicit Transaction(txn_id_t txn_id,
                         IsolationLevel isolation = IsolationLevel::REPEATABLE_READ);

    ~Transaction() = default;

    // Non-copyable, movable
    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;
    Transaction(Transaction&&) = default;
    Transaction& operator=(Transaction&&) = default;

    // ─────────────────────────────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────────────────────────────

    [[nodiscard]] txn_id_t txn_id() const noexcept { return txn_id_; }
    [[nodiscard]] TransactionState state() const noexcept { return state_; }
    [[nodiscard]] IsolationLevel isolation_level() const noexcept { return isolation_level_; }

    /**
     * @brief Get the LSN of the previous log record for this transaction
     * Used for linking log records in the WAL
     */
    [[nodiscard]] lsn_t prev_lsn() const noexcept { return prev_lsn_; }

    /**
     * @brief Get transaction start timestamp (for MVCC)
     */
    [[nodiscard]] uint64_t start_ts() const noexcept { return start_ts_; }

    /**
     * @brief Get transaction commit timestamp (for MVCC)
     * Only valid after commit
     */
    [[nodiscard]] uint64_t commit_ts() const noexcept { return commit_ts_; }

    // ─────────────────────────────────────────────────────────────────────────
    // State Management
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Set transaction state
     * @note Should only be called by TransactionManager
     */
    void set_state(TransactionState state) noexcept { state_ = state; }

    /**
     * @brief Set the previous LSN (last log record for this transaction)
     */
    void set_prev_lsn(lsn_t lsn) noexcept { prev_lsn_ = lsn; }

    /**
     * @brief Set commit timestamp
     */
    void set_commit_ts(uint64_t ts) noexcept { commit_ts_ = ts; }

    /**
     * @brief Check if transaction is in a state where it can perform operations
     */
    [[nodiscard]] bool is_active() const noexcept {
        return state_ == TransactionState::GROWING || state_ == TransactionState::SHRINKING;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Write Set Management
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Record a write operation for potential rollback
     */
    void add_write_record(WriteRecord record) { write_set_.push_back(std::move(record)); }

    /**
     * @brief Get the write set (for rollback)
     */
    [[nodiscard]] const std::vector<WriteRecord>& write_set() const noexcept {
        return write_set_;
    }

    /**
     * @brief Get mutable write set
     */
    [[nodiscard]] std::vector<WriteRecord>& write_set() noexcept { return write_set_; }

    /**
     * @brief Clear the write set (after successful commit)
     */
    void clear_write_set() noexcept { write_set_.clear(); }

    // ─────────────────────────────────────────────────────────────────────────
    // Lock Tracking (for 2PL)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Record that this transaction holds a lock on a page
     */
    void add_page_lock(page_id_t page_id) { page_locks_.insert(page_id); }

    /**
     * @brief Check if transaction holds lock on page
     */
    [[nodiscard]] bool has_page_lock(page_id_t page_id) const {
        return page_locks_.count(page_id) > 0;
    }

    /**
     * @brief Get all page locks held
     */
    [[nodiscard]] const std::unordered_set<page_id_t>& page_locks() const noexcept {
        return page_locks_;
    }

    /**
     * @brief Record that this transaction holds a lock on a table
     */
    void add_table_lock(oid_t table_oid) { table_locks_.insert(table_oid); }

    /**
     * @brief Get all table locks held
     */
    [[nodiscard]] const std::unordered_set<oid_t>& table_locks() const noexcept {
        return table_locks_;
    }

private:
    txn_id_t txn_id_;
    TransactionState state_ = TransactionState::GROWING;
    IsolationLevel isolation_level_;

    // WAL linkage
    lsn_t prev_lsn_ = INVALID_LSN;

    // MVCC timestamps
    uint64_t start_ts_;
    uint64_t commit_ts_ = 0;

    // Write set for undo
    std::vector<WriteRecord> write_set_;

    // Lock tracking for 2PL
    std::unordered_set<page_id_t> page_locks_;
    std::unordered_set<oid_t> table_locks_;
};

}  // namespace entropy
