#pragma once

/**
 * @file lock_manager.hpp
 * @brief Lock management for Two-Phase Locking (2PL)
 *
 * The Lock Manager implements row-level and table-level locking with:
 * - Shared (S) and Exclusive (X) lock modes
 * - Lock request queues for blocking
 * - Basic deadlock detection using wait-for graph
 * - 2PL protocol enforcement
 *
 * Lock Compatibility Matrix:
 *          | S | X |
 *       S  | Y | N |
 *       X  | N | N |
 */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <list>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/types.hpp"
#include "entropy/status.hpp"

namespace entropy {

// Forward declarations
class Transaction;

// ─────────────────────────────────────────────────────────────────────────────
// Lock Modes
// ─────────────────────────────────────────────────────────────────────────────

enum class LockMode : uint8_t {
    SHARED = 0,     // Read lock - multiple transactions can hold
    EXCLUSIVE = 1   // Write lock - only one transaction can hold
};

/**
 * @brief Convert lock mode to string for debugging
 */
[[nodiscard]] inline const char* lock_mode_to_string(LockMode mode) {
    switch (mode) {
        case LockMode::SHARED:
            return "SHARED";
        case LockMode::EXCLUSIVE:
            return "EXCLUSIVE";
        default:
            return "UNKNOWN";
    }
}

/**
 * @brief Check if two lock modes are compatible
 *
 * Compatibility matrix:
 *   - S and S: compatible (multiple readers)
 *   - S and X: incompatible
 *   - X and X: incompatible
 */
[[nodiscard]] inline bool are_lock_modes_compatible(LockMode mode1, LockMode mode2) {
    return mode1 == LockMode::SHARED && mode2 == LockMode::SHARED;
}

// ─────────────────────────────────────────────────────────────────────────────
// Lock Request
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Represents a transaction's request for a lock
 */
struct LockRequest {
    txn_id_t txn_id;          // Transaction requesting the lock
    LockMode mode;            // Requested lock mode
    bool granted = false;     // Whether the lock has been granted

    LockRequest(txn_id_t tid, LockMode m) : txn_id(tid), mode(m) {}
};

// ─────────────────────────────────────────────────────────────────────────────
// Lock Request Queue
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Queue of lock requests for a single resource (table or row)
 *
 * Maintains a list of requests in FIFO order. Granted requests are at the front,
 * followed by waiting requests.
 */
struct LockRequestQueue {
    std::list<LockRequest> request_queue;
    std::condition_variable cv;      // For waiting transactions
    bool upgrading = false;          // Is a lock upgrade in progress?
    txn_id_t upgrading_txn_id = INVALID_TXN_ID;  // Who's upgrading?

    /**
     * @brief Find a request by transaction ID
     * @return Iterator to the request, or end() if not found
     */
    [[nodiscard]] std::list<LockRequest>::iterator find_request(txn_id_t txn_id) {
        for (auto it = request_queue.begin(); it != request_queue.end(); ++it) {
            if (it->txn_id == txn_id) {
                return it;
            }
        }
        return request_queue.end();
    }

    /**
     * @brief Count granted requests
     */
    [[nodiscard]] size_t granted_count() const {
        size_t count = 0;
        for (const auto& req : request_queue) {
            if (req.granted) {
                ++count;
            }
        }
        return count;
    }

    /**
     * @brief Check if the first waiting request can be granted
     */
    [[nodiscard]] bool can_grant_first_waiting() const {
        // Find first waiting request
        const LockRequest* first_waiting = nullptr;
        for (const auto& req : request_queue) {
            if (!req.granted) {
                first_waiting = &req;
                break;
            }
        }

        if (first_waiting == nullptr) {
            return false;  // No waiting requests
        }

        // Check compatibility with all granted requests
        for (const auto& req : request_queue) {
            if (req.granted && !are_lock_modes_compatible(req.mode, first_waiting->mode)) {
                return false;  // Incompatible with a granted request
            }
        }

        return true;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Lock Target - identifies what's being locked
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Identifies a lock target (table or row)
 *
 * For table locks: table_oid is set, rid is invalid
 * For row locks: both table_oid and rid are set
 */
struct LockTarget {
    oid_t table_oid;
    RID rid;  // Invalid RID means table-level lock

    LockTarget(oid_t tbl, RID r = RID{}) : table_oid(tbl), rid(r) {}

    bool operator==(const LockTarget& other) const {
        return table_oid == other.table_oid && rid == other.rid;
    }

    [[nodiscard]] bool is_table_lock() const {
        return !rid.is_valid();
    }
};

}  // namespace entropy

// Hash support for LockTarget
namespace std {
template <>
struct hash<entropy::LockTarget> {
    size_t operator()(const entropy::LockTarget& target) const noexcept {
        size_t h1 = hash<entropy::oid_t>{}(target.table_oid);
        size_t h2 = hash<entropy::RID>{}(target.rid);
        return h1 ^ (h2 << 1);
    }
};
}  // namespace std

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Lock Manager
// ─────────────────────────────────────────────────────────────────────────────

/**
 * @brief Manages locks for 2PL concurrency control
 *
 * Thread safety: All public methods are thread-safe.
 *
 * Usage:
 *   auto status = lock_mgr.lock_row(txn, table_oid, rid, LockMode::SHARED);
 *   if (!status.ok()) {
 *       // Handle lock failure (abort transaction)
 *   }
 *   // ... do work ...
 *   lock_mgr.unlock_row(txn, table_oid, rid);
 *
 * Deadlock detection:
 *   The lock manager uses a wait-for graph to detect deadlocks.
 *   When a deadlock is detected, one of the transactions is aborted.
 */
class LockManager {
public:
    /**
     * @brief Default lock wait timeout (milliseconds)
     */
    static constexpr uint32_t DEFAULT_LOCK_TIMEOUT_MS = 5000;

    /**
     * @brief Create a lock manager
     * @param enable_deadlock_detection Whether to enable deadlock detection
     * @param lock_timeout_ms Maximum time to wait for a lock
     */
    explicit LockManager(bool enable_deadlock_detection = true,
                        uint32_t lock_timeout_ms = DEFAULT_LOCK_TIMEOUT_MS);

    ~LockManager() = default;

    // Non-copyable
    LockManager(const LockManager&) = delete;
    LockManager& operator=(const LockManager&) = delete;

    // ─────────────────────────────────────────────────────────────────────────
    // Table Locks
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Acquire a table-level lock
     * @param txn The transaction requesting the lock
     * @param table_oid The table to lock
     * @param mode Lock mode (SHARED or EXCLUSIVE)
     * @return Status::Ok() on success
     *         Status::Aborted() if transaction was aborted (deadlock)
     *         Status::Timeout() if lock wait timed out
     *         Status::InvalidArgument() if transaction is invalid
     */
    [[nodiscard]] Status lock_table(Transaction* txn, oid_t table_oid, LockMode mode);

    /**
     * @brief Release a table-level lock
     * @return Status::Ok() on success
     *         Status::NotFound() if lock was not held
     */
    [[nodiscard]] Status unlock_table(Transaction* txn, oid_t table_oid);

    // ─────────────────────────────────────────────────────────────────────────
    // Row Locks
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Acquire a row-level lock
     * @param txn The transaction requesting the lock
     * @param table_oid The table containing the row
     * @param rid The row to lock
     * @param mode Lock mode (SHARED or EXCLUSIVE)
     * @return Status::Ok() on success
     *         Status::Aborted() if transaction was aborted (deadlock)
     *         Status::Timeout() if lock wait timed out
     *         Status::InvalidArgument() if transaction is invalid
     */
    [[nodiscard]] Status lock_row(Transaction* txn, oid_t table_oid, const RID& rid,
                                  LockMode mode);

    /**
     * @brief Release a row-level lock
     */
    [[nodiscard]] Status unlock_row(Transaction* txn, oid_t table_oid, const RID& rid);

    // ─────────────────────────────────────────────────────────────────────────
    // Lock Upgrade
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Upgrade a lock from SHARED to EXCLUSIVE
     * @note This is an atomic operation - the lock is never released during upgrade
     */
    [[nodiscard]] Status upgrade_lock(Transaction* txn, oid_t table_oid,
                                      const RID& rid = RID{});

    // ─────────────────────────────────────────────────────────────────────────
    // Transaction Cleanup
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Release all locks held by a transaction
     * @note Called during commit or abort
     */
    void release_all_locks(Transaction* txn);

    // ─────────────────────────────────────────────────────────────────────────
    // Statistics
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Get the number of lock targets currently in the lock table
     */
    [[nodiscard]] size_t lock_table_size() const;

    /**
     * @brief Get the number of deadlocks detected
     */
    [[nodiscard]] uint64_t deadlock_count() const noexcept {
        return deadlock_count_.load(std::memory_order_relaxed);
    }

private:
    // ─────────────────────────────────────────────────────────────────────────
    // Internal Lock Methods
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Internal implementation of lock acquisition
     */
    [[nodiscard]] Status lock_internal(Transaction* txn, const LockTarget& target,
                                       LockMode mode);

    /**
     * @brief Internal implementation of lock release
     */
    [[nodiscard]] Status unlock_internal(Transaction* txn, const LockTarget& target);

    /**
     * @brief Try to grant locks to waiting transactions
     */
    void grant_waiting_locks(LockRequestQueue* queue);

    // ─────────────────────────────────────────────────────────────────────────
    // Deadlock Detection
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * @brief Check if waiting for a lock would cause a deadlock
     * @param waiting_txn_id Transaction that would be waiting
     * @param queue The lock request queue being waited on
     * @return True if a deadlock would occur
     */
    [[nodiscard]] bool would_cause_deadlock(txn_id_t waiting_txn_id,
                                            const LockRequestQueue* queue);

    /**
     * @brief Build wait-for edges: which transactions is this one waiting for?
     */
    [[nodiscard]] std::vector<txn_id_t> get_blocking_txns(txn_id_t waiting_txn_id,
                                                          const LockRequestQueue* queue) const;

    /**
     * @brief DFS to detect cycle in wait-for graph
     */
    [[nodiscard]] bool has_cycle(txn_id_t start_txn_id,
                                 const std::unordered_map<txn_id_t, std::vector<txn_id_t>>& wait_for_graph,
                                 std::unordered_set<txn_id_t>& visited,
                                 std::unordered_set<txn_id_t>& rec_stack) const;

    // ─────────────────────────────────────────────────────────────────────────
    // Data Members
    // ─────────────────────────────────────────────────────────────────────────

    /// Lock table: target -> request queue
    std::unordered_map<LockTarget, std::unique_ptr<LockRequestQueue>> lock_table_;

    /// Protects the lock table
    mutable std::mutex latch_;

    /// Configuration
    bool enable_deadlock_detection_;
    std::chrono::milliseconds lock_timeout_;

    /// Statistics
    std::atomic<uint64_t> deadlock_count_{0};
};

}  // namespace entropy
