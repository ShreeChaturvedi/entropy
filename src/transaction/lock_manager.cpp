/**
 * @file lock_manager.cpp
 * @brief Lock Manager implementation
 */

#include "transaction/lock_manager.hpp"

#include <algorithm>

#include "common/logger.hpp"
#include "transaction/transaction.hpp"

namespace entropy {

LockManager::LockManager(bool enable_deadlock_detection, uint32_t lock_timeout_ms)
    : enable_deadlock_detection_(enable_deadlock_detection),
      lock_timeout_(lock_timeout_ms) {}

// ─────────────────────────────────────────────────────────────────────────────
// Public API - Table Locks
// ─────────────────────────────────────────────────────────────────────────────

Status LockManager::lock_table(Transaction* txn, oid_t table_oid, LockMode mode) {
    return lock_internal(txn, LockTarget{table_oid}, mode);
}

Status LockManager::unlock_table(Transaction* txn, oid_t table_oid) {
    return unlock_internal(txn, LockTarget{table_oid});
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API - Row Locks
// ─────────────────────────────────────────────────────────────────────────────

Status LockManager::lock_row(Transaction* txn, oid_t table_oid, const RID& rid,
                              LockMode mode) {
    return lock_internal(txn, LockTarget{table_oid, rid}, mode);
}

Status LockManager::unlock_row(Transaction* txn, oid_t table_oid, const RID& rid) {
    return unlock_internal(txn, LockTarget{table_oid, rid});
}

// ─────────────────────────────────────────────────────────────────────────────
// Lock Upgrade
// ─────────────────────────────────────────────────────────────────────────────

Status LockManager::upgrade_lock(Transaction* txn, oid_t table_oid, const RID& rid) {
    if (txn == nullptr) {
        return Status::InvalidArgument("Transaction is null");
    }

    if (!txn->is_active()) {
        return Status::Aborted("Transaction is not active");
    }

    LockTarget target{table_oid, rid};
    std::unique_lock<std::mutex> lock(latch_);

    // Find the lock request queue
    auto it = lock_table_.find(target);
    if (it == lock_table_.end()) {
        return Status::NotFound("No lock held on target");
    }

    LockRequestQueue* queue = it->second.get();

    // Find the transaction's request
    auto req_it = queue->find_request(txn->txn_id());
    if (req_it == queue->request_queue.end()) {
        return Status::NotFound("Transaction does not hold lock on target");
    }

    // Already exclusive?
    if (req_it->mode == LockMode::EXCLUSIVE) {
        return Status::Ok();  // Nothing to do
    }

    // Must be granted shared lock
    if (!req_it->granted) {
        return Status::InvalidArgument("Cannot upgrade a waiting lock request");
    }

    // Check if another transaction is already upgrading
    if (queue->upgrading) {
        // Two transactions trying to upgrade = deadlock
        LOG_DEBUG("Deadlock detected: multiple upgrade requests for txn {} and {}",
                  txn->txn_id(), queue->upgrading_txn_id);
        deadlock_count_.fetch_add(1, std::memory_order_relaxed);
        return Status::Aborted("Deadlock: multiple upgrade requests");
    }

    // Check if we can upgrade immediately
    // We can upgrade if we're the only holder OR all other holders are shared
    bool can_upgrade_now = true;
    for (const auto& req : queue->request_queue) {
        if (req.txn_id != txn->txn_id() && req.granted) {
            // Another transaction holds the lock
            can_upgrade_now = false;
            break;
        }
    }

    if (can_upgrade_now) {
        req_it->mode = LockMode::EXCLUSIVE;
        LOG_DEBUG("Txn {} upgraded lock to EXCLUSIVE immediately", txn->txn_id());
        return Status::Ok();
    }

    // Need to wait for other holders to release
    queue->upgrading = true;
    queue->upgrading_txn_id = txn->txn_id();

    // Check for deadlock before waiting
    if (enable_deadlock_detection_ && would_cause_deadlock(txn->txn_id(), queue)) {
        queue->upgrading = false;
        queue->upgrading_txn_id = INVALID_TXN_ID;
        deadlock_count_.fetch_add(1, std::memory_order_relaxed);
        LOG_DEBUG("Deadlock detected during upgrade for txn {}", txn->txn_id());
        return Status::Aborted("Deadlock detected during lock upgrade");
    }

    // Wait for upgrade
    auto deadline = std::chrono::steady_clock::now() + lock_timeout_;
    while (true) {
        // Check if we can upgrade
        bool all_others_released = true;
        for (const auto& req : queue->request_queue) {
            if (req.txn_id != txn->txn_id() && req.granted) {
                all_others_released = false;
                break;
            }
        }

        if (all_others_released) {
            req_it->mode = LockMode::EXCLUSIVE;
            queue->upgrading = false;
            queue->upgrading_txn_id = INVALID_TXN_ID;
            LOG_DEBUG("Txn {} upgraded lock to EXCLUSIVE after wait", txn->txn_id());
            return Status::Ok();
        }

        // Wait with timeout
        if (queue->cv.wait_until(lock, deadline) == std::cv_status::timeout) {
            queue->upgrading = false;
            queue->upgrading_txn_id = INVALID_TXN_ID;
            LOG_DEBUG("Txn {} timed out waiting for lock upgrade", txn->txn_id());
            return Status::Timeout("Lock upgrade timed out");
        }

        // Check if transaction was aborted
        if (!txn->is_active()) {
            queue->upgrading = false;
            queue->upgrading_txn_id = INVALID_TXN_ID;
            return Status::Aborted("Transaction was aborted");
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Transaction Cleanup
// ─────────────────────────────────────────────────────────────────────────────

void LockManager::release_all_locks(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }

    std::lock_guard<std::mutex> lock(latch_);

    // Find all locks held by this transaction and remove them
    std::vector<LockTarget> targets_to_remove;

    for (auto& [target, queue] : lock_table_) {
        auto req_it = queue->find_request(txn->txn_id());
        if (req_it != queue->request_queue.end()) {
            bool was_granted = req_it->granted;
            queue->request_queue.erase(req_it);

            // If this was the upgrading transaction, clear upgrade flag
            if (queue->upgrading && queue->upgrading_txn_id == txn->txn_id()) {
                queue->upgrading = false;
                queue->upgrading_txn_id = INVALID_TXN_ID;
            }

            // If we removed a granted request, try to grant waiting requests
            if (was_granted && !queue->request_queue.empty()) {
                grant_waiting_locks(queue.get());
                queue->cv.notify_all();
            }

            // Mark empty queues for removal
            if (queue->request_queue.empty()) {
                targets_to_remove.push_back(target);
            }
        }
    }

    // Remove empty queues
    for (const auto& target : targets_to_remove) {
        lock_table_.erase(target);
    }

    LOG_DEBUG("Released all locks for txn {}", txn->txn_id());
}

// ─────────────────────────────────────────────────────────────────────────────
// Statistics
// ─────────────────────────────────────────────────────────────────────────────

size_t LockManager::lock_table_size() const {
    std::lock_guard<std::mutex> lock(latch_);
    return lock_table_.size();
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal Lock Implementation
// ─────────────────────────────────────────────────────────────────────────────

Status LockManager::lock_internal(Transaction* txn, const LockTarget& target,
                                   LockMode mode) {
    if (txn == nullptr) {
        return Status::InvalidArgument("Transaction is null");
    }

    if (!txn->is_active()) {
        return Status::Aborted("Transaction is not active");
    }

    // Check 2PL: cannot acquire locks in shrinking phase
    if (txn->state() == TransactionState::SHRINKING) {
        return Status::Aborted("Cannot acquire locks in shrinking phase (2PL violation)");
    }

    std::unique_lock<std::mutex> lock(latch_);

    // Get or create request queue for this target
    auto& queue_ptr = lock_table_[target];
    if (!queue_ptr) {
        queue_ptr = std::make_unique<LockRequestQueue>();
    }
    LockRequestQueue* queue = queue_ptr.get();

    // Check if this transaction already has a lock on this target
    auto req_it = queue->find_request(txn->txn_id());
    if (req_it != queue->request_queue.end()) {
        // Already have a request
        if (req_it->granted) {
            // Already have the lock
            if (req_it->mode == mode || req_it->mode == LockMode::EXCLUSIVE) {
                // Same mode or already have stronger lock
                return Status::Ok();
            }
            // Requesting exclusive but have shared - need upgrade
            // Release latch and call upgrade
            lock.unlock();
            return upgrade_lock(txn, target.table_oid, target.rid);
        }
        // Already waiting - shouldn't happen in normal usage
        return Status::Busy("Transaction already waiting for this lock");
    }

    // Create new lock request
    queue->request_queue.emplace_back(txn->txn_id(), mode);
    auto new_req_it = std::prev(queue->request_queue.end());

    // Can we grant immediately?
    bool can_grant = true;

    // Check upgrade in progress - upgrading transaction has priority
    if (queue->upgrading && mode == LockMode::EXCLUSIVE) {
        can_grant = false;
    }

    // Check compatibility with granted locks
    if (can_grant) {
        for (const auto& req : queue->request_queue) {
            if (&req == &*new_req_it) {
                continue;  // Skip self
            }
            if (req.granted && !are_lock_modes_compatible(req.mode, mode)) {
                can_grant = false;
                break;
            }
        }
    }

    // Check FIFO order for waiting requests (no queue jumping)
    if (can_grant) {
        for (const auto& req : queue->request_queue) {
            if (&req == &*new_req_it) {
                break;  // We're checking requests before us
            }
            if (!req.granted) {
                // There's a waiting request before us
                if (!are_lock_modes_compatible(req.mode, mode)) {
                    can_grant = false;
                    break;
                }
            }
        }
    }

    if (can_grant) {
        new_req_it->granted = true;
        LOG_DEBUG("Txn {} acquired {} lock immediately on table {} row ({},{})",
                  txn->txn_id(), lock_mode_to_string(mode), target.table_oid,
                  target.rid.page_id, target.rid.slot_id);
        return Status::Ok();
    }

    // Need to wait - check for deadlock first
    if (enable_deadlock_detection_ && would_cause_deadlock(txn->txn_id(), queue)) {
        queue->request_queue.erase(new_req_it);
        if (queue->request_queue.empty()) {
            lock_table_.erase(target);
        }
        deadlock_count_.fetch_add(1, std::memory_order_relaxed);
        LOG_DEBUG("Deadlock detected for txn {}", txn->txn_id());
        return Status::Aborted("Deadlock detected");
    }

    // Wait for lock
    LOG_DEBUG("Txn {} waiting for {} lock on table {} row ({},{})",
              txn->txn_id(), lock_mode_to_string(mode), target.table_oid,
              target.rid.page_id, target.rid.slot_id);

    auto deadline = std::chrono::steady_clock::now() + lock_timeout_;
    while (!new_req_it->granted) {
        if (queue->cv.wait_until(lock, deadline) == std::cv_status::timeout) {
            // Timeout - remove request and return
            queue->request_queue.erase(new_req_it);
            if (queue->request_queue.empty()) {
                lock_table_.erase(target);
            }
            LOG_DEBUG("Txn {} timed out waiting for lock", txn->txn_id());
            return Status::Timeout("Lock acquisition timed out");
        }

        // Check if transaction was aborted while waiting
        if (!txn->is_active()) {
            queue->request_queue.erase(new_req_it);
            if (queue->request_queue.empty()) {
                lock_table_.erase(target);
            }
            return Status::Aborted("Transaction was aborted");
        }
    }

    LOG_DEBUG("Txn {} acquired {} lock after wait on table {} row ({},{})",
              txn->txn_id(), lock_mode_to_string(mode), target.table_oid,
              target.rid.page_id, target.rid.slot_id);
    return Status::Ok();
}

Status LockManager::unlock_internal(Transaction* txn, const LockTarget& target) {
    if (txn == nullptr) {
        return Status::InvalidArgument("Transaction is null");
    }

    std::lock_guard<std::mutex> lock(latch_);

    // Find the queue
    auto queue_it = lock_table_.find(target);
    if (queue_it == lock_table_.end()) {
        return Status::NotFound("No lock held on target");
    }

    LockRequestQueue* queue = queue_it->second.get();

    // Find the transaction's request
    auto req_it = queue->find_request(txn->txn_id());
    if (req_it == queue->request_queue.end()) {
        return Status::NotFound("Transaction does not hold lock on target");
    }

    if (!req_it->granted) {
        // Remove waiting request
        queue->request_queue.erase(req_it);
        if (queue->request_queue.empty()) {
            lock_table_.erase(target);
        }
        return Status::Ok();
    }

    // Releasing a granted lock - transition to shrinking phase (2PL)
    if (txn->state() == TransactionState::GROWING) {
        txn->set_state(TransactionState::SHRINKING);
        LOG_DEBUG("Txn {} transitioning to SHRINKING phase", txn->txn_id());
    }

    // Remove the request
    queue->request_queue.erase(req_it);

    // Try to grant waiting requests
    if (!queue->request_queue.empty()) {
        grant_waiting_locks(queue);
        queue->cv.notify_all();
    } else {
        // No more requests - remove queue
        lock_table_.erase(target);
    }

    LOG_DEBUG("Txn {} released lock on table {} row ({},{})",
              txn->txn_id(), target.table_oid, target.rid.page_id, target.rid.slot_id);
    return Status::Ok();
}

void LockManager::grant_waiting_locks(LockRequestQueue* queue) {
    // Grant as many waiting requests as possible
    // For FIFO fairness, grant in order

    // First, handle any upgrading transaction
    if (queue->upgrading) {
        // Check if upgrade can proceed
        bool can_upgrade = true;
        for (const auto& req : queue->request_queue) {
            if (req.txn_id != queue->upgrading_txn_id && req.granted) {
                can_upgrade = false;
                break;
            }
        }
        if (can_upgrade) {
            // Upgrade will be completed by the waiting thread
            // Just notify
            return;
        }
    }

    // Grant waiting requests in order
    for (auto& req : queue->request_queue) {
        if (req.granted) {
            continue;  // Already granted
        }

        // Check compatibility with granted requests
        bool can_grant = true;
        for (const auto& other : queue->request_queue) {
            if (&other == &req) {
                continue;
            }
            if (other.granted && !are_lock_modes_compatible(other.mode, req.mode)) {
                can_grant = false;
                break;
            }
        }

        if (can_grant) {
            req.granted = true;
            LOG_DEBUG("Granted waiting {} lock to txn {}",
                      lock_mode_to_string(req.mode), req.txn_id);
        } else {
            // Can't grant this one - stop (FIFO order for exclusive)
            // But can continue for shared requests
            if (req.mode == LockMode::EXCLUSIVE) {
                break;
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Deadlock Detection
// ─────────────────────────────────────────────────────────────────────────────

bool LockManager::would_cause_deadlock(txn_id_t waiting_txn_id,
                                        const LockRequestQueue* queue) {
    // Build a wait-for graph by looking at all lock queues
    // Then check if adding this edge would create a cycle

    std::unordered_map<txn_id_t, std::vector<txn_id_t>> wait_for_graph;

    // Get transactions that waiting_txn would wait for
    auto blocking = get_blocking_txns(waiting_txn_id, queue);
    if (!blocking.empty()) {
        wait_for_graph[waiting_txn_id] = blocking;
    }

    // Build wait-for edges for all other waiting transactions
    for (const auto& [target, q] : lock_table_) {
        for (const auto& req : q->request_queue) {
            if (!req.granted && req.txn_id != waiting_txn_id) {
                auto others = get_blocking_txns(req.txn_id, q.get());
                if (!others.empty()) {
                    wait_for_graph[req.txn_id] = others;
                }
            }
        }
    }

    // DFS to find cycle starting from waiting_txn_id
    std::unordered_set<txn_id_t> visited;
    std::unordered_set<txn_id_t> rec_stack;

    return has_cycle(waiting_txn_id, wait_for_graph, visited, rec_stack);
}

std::vector<txn_id_t> LockManager::get_blocking_txns(
    txn_id_t waiting_txn_id, const LockRequestQueue* queue) const {
    std::vector<txn_id_t> blocking;

    // Find the waiting transaction's request
    const LockRequest* waiting_req = nullptr;
    for (const auto& req : queue->request_queue) {
        if (req.txn_id == waiting_txn_id) {
            waiting_req = &req;
            break;
        }
    }

    if (waiting_req == nullptr || waiting_req->granted) {
        return blocking;  // Not waiting
    }

    // Find all granted transactions that are blocking this one
    for (const auto& req : queue->request_queue) {
        if (req.granted && !are_lock_modes_compatible(req.mode, waiting_req->mode)) {
            blocking.push_back(req.txn_id);
        }
    }

    return blocking;
}

bool LockManager::has_cycle(
    txn_id_t start_txn_id,
    const std::unordered_map<txn_id_t, std::vector<txn_id_t>>& wait_for_graph,
    std::unordered_set<txn_id_t>& visited,
    std::unordered_set<txn_id_t>& rec_stack) const {

    visited.insert(start_txn_id);
    rec_stack.insert(start_txn_id);

    auto it = wait_for_graph.find(start_txn_id);
    if (it != wait_for_graph.end()) {
        for (txn_id_t next : it->second) {
            if (rec_stack.count(next) > 0) {
                // Found cycle
                return true;
            }
            if (visited.count(next) == 0) {
                if (has_cycle(next, wait_for_graph, visited, rec_stack)) {
                    return true;
                }
            }
        }
    }

    rec_stack.erase(start_txn_id);
    return false;
}

}  // namespace entropy
