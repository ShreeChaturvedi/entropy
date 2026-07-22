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

    // Another transaction already owns the single upgrade slot on this queue.
    // Two transactions upgrading the same resource deadlock (each holds S and
    // waits for the other's S to drop). Resolve with wait-die.
    if (queue->upgrading && queue->upgrading_txn_id != txn->txn_id()) {
        deadlock_count_.fetch_add(1, std::memory_order_relaxed);
        if (txn->txn_id() > queue->upgrading_txn_id) {
            // Younger requester loses. We KEEP our granted locks: they are
            // released by TransactionManager::abort() only after write-set
            // undo, so survivors can never observe not-yet-rolled-back data.
            LOG_DEBUG("Deadlock: txn {} loses upgrade race with {} (wait-die)",
                      txn->txn_id(), queue->upgrading_txn_id);
            txn->set_aborted_by_deadlock();
            txn->set_state(TransactionState::ABORTED);
            return Status::Aborted(
                "Deadlock detected during lock upgrade (wait-die victim)");
        }
        // Older requester wins: abort the younger upgrader and take the slot.
        LOG_DEBUG("Deadlock: txn {} aborts younger upgrader {} (wait-die)",
                  txn->txn_id(), queue->upgrading_txn_id);
        if (Transaction* other = find_transaction_locked(queue->upgrading_txn_id)) {
            abort_victim_locked(other);
        }
        queue->upgrading = false;
        queue->upgrading_txn_id = INVALID_TXN_ID;
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

    // Wait for the other holders to release, re-checking for deadlock on every
    // wake so a cycle that forms while we sit in the upgrade slot is resolved.
    //
    // NOTE on the upgrade slot: an OLDER upgrader that aborts us (wait-die)
    // takes the slot for itself, so on any exit path where we may already be
    // aborted the slot must only be cleared if it is still ours.
    //
    // NOTE on aborted exits: we KEEP our granted locks (only the upgrade slot
    // is relinquished). TransactionManager::abort() releases them AFTER
    // write-set undo, so survivors can never acquire our locks and observe
    // (or overwrite) not-yet-rolled-back data. Nothing becomes grantable from
    // dropping the slot alone while our SHARED lock is still held, so no
    // re-grant pass is needed here.
    const auto abandon_upgrade_slot = [&]() {
        if (queue->upgrading && queue->upgrading_txn_id == txn->txn_id()) {
            queue->upgrading = false;
            queue->upgrading_txn_id = INVALID_TXN_ID;
        }
    };

    auto deadline = std::chrono::steady_clock::now() + lock_timeout_;
    while (true) {
        // Aborted by another thread as a deadlock victim: never complete the
        // upgrade.
        if (!txn->is_active()) {
            abandon_upgrade_slot();
            return Status::Aborted("Transaction was aborted");
        }

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

        // Deadlock re-check for the queued upgrade (wait-die victim selection).
        if (enable_deadlock_detection_) {
            txn_id_t victim = select_deadlock_victim(txn->txn_id());
            if (victim != INVALID_TXN_ID) {
                deadlock_count_.fetch_add(1, std::memory_order_relaxed);
                if (victim == txn->txn_id()) {
                    txn->set_aborted_by_deadlock();
                    txn->set_state(TransactionState::ABORTED);
                    abandon_upgrade_slot();
                    return Status::Aborted(
                        "Deadlock detected during lock upgrade (wait-die victim)");
                }
                if (Transaction* victim_txn = find_transaction_locked(victim)) {
                    abort_victim_locked(victim_txn);
                }
                // Fall through to wait so the victim can run and release.
            }
        }

        // Wait with timeout
        if (queue->cv.wait_until(lock, deadline) == std::cv_status::timeout) {
            abandon_upgrade_slot();
            if (!txn->is_active()) {
                // Aborted right as the deadline passed; take the abort path.
                return Status::Aborted("Transaction was aborted");
            }
            LOG_DEBUG("Txn {} timed out waiting for lock upgrade", txn->txn_id());
            return Status::Timeout("Lock upgrade timed out");
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
    release_all_locks_locked(txn);
}

void LockManager::release_all_locks_locked(Transaction* txn) {
    // Find all locks held by this transaction and remove them
    std::vector<LockTarget> targets_to_remove;

    for (auto& [target, queue] : lock_table_) {
        auto req_it = queue->find_request(txn->txn_id());
        if (req_it != queue->request_queue.end()) {
            queue->request_queue.erase(req_it);

            // If this was the upgrading transaction, clear upgrade flag
            if (queue->upgrading && queue->upgrading_txn_id == txn->txn_id()) {
                queue->upgrading = false;
                queue->upgrading_txn_id = INVALID_TXN_ID;
            }

            // Re-evaluate grants whether the erased request was granted or
            // waiting: a waiting request FIFO-gates later compatible requests,
            // so its removal can unblock them too.
            if (!queue->request_queue.empty()) {
                grant_waiting_locks(queue.get());
                queue->cv.notify_all();
            } else {
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

void LockManager::erase_request_locked(LockRequestQueue* queue,
                                       std::list<LockRequest>::iterator req_it,
                                       const LockTarget& target) {
    queue->request_queue.erase(req_it);
    if (queue->request_queue.empty()) {
        lock_table_.erase(target);
        return;
    }
    // Erasing either a granted or a waiting request can unblock others (a
    // waiting request FIFO-gates later compatible requests).
    grant_waiting_locks(queue);
    queue->cv.notify_all();
}

void LockManager::abort_victim_locked(Transaction* victim) {
    // A deadlock-cycle victim is, by definition, blocked inside one of its own
    // wait loops (lock_internal or upgrade_lock). We do NOT touch its requests
    // from here: those loops retain list iterators into the queues, so erasing
    // cross-thread would be undefined behaviour. Instead we mark it ABORTED and
    // wake every queue it participates in. Its own thread then observes the
    // aborted state, erases only its WAITING request, and returns Aborted; its
    // GRANTED locks stay held until TransactionManager::abort() releases them
    // AFTER write-set undo. Survivors therefore unblock as soon as the victim's
    // rollback completes (microseconds, not the multi-second wait timeout) and
    // can never acquire the victim's locks while aborted data is still in the
    // heap.
    //
    // The deadlock mark tells TransactionManager::abort() that this ABORTED
    // state still needs finalization (write-set undo, WAL ABORT, removal from
    // the active set) when the victim's thread calls it.
    victim->set_aborted_by_deadlock();
    victim->set_state(TransactionState::ABORTED);

    const txn_id_t victim_id = victim->txn_id();
    for (auto& [target, queue] : lock_table_) {
        if (queue->find_request(victim_id) != queue->request_queue.end()) {
            queue->cv.notify_all();
        }
    }

    LOG_DEBUG("Signalled deadlock victim txn {} to self-abort", victim_id);
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
    queue->request_queue.emplace_back(txn->txn_id(), mode, txn);
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

    // Wait for lock
    LOG_DEBUG("Txn {} waiting for {} lock on table {} row ({},{})",
              txn->txn_id(), lock_mode_to_string(mode), target.table_oid,
              target.rid.page_id, target.rid.slot_id);

    // The deadlock check runs before the first wait AND again on every wake:
    // aborting one victim can leave a second, overlapping cycle through this
    // same resource intact (e.g. we wait on a lock held under S by two txns,
    // each closing its own cycle with us), and new cycles can form while we
    // sleep. The request is already queued as a (not-yet-granted) waiter, so
    // the wait-for graph reflects our edge. Rebuilding the full graph and
    // running a DFS under latch_ on every wake is a deliberate
    // simplicity-over-throughput trade-off at this engine's scale.
    //
    // Aborted exits below erase ONLY our waiting request; our granted locks
    // stay held until TransactionManager::abort() releases them AFTER
    // write-set undo, so survivors can never acquire our locks and observe
    // (or overwrite) not-yet-rolled-back data.
    auto deadline = std::chrono::steady_clock::now() + lock_timeout_;
    while (!new_req_it->granted) {
        if (enable_deadlock_detection_) {
            txn_id_t victim = select_deadlock_victim(txn->txn_id());
            if (victim != INVALID_TXN_ID) {
                deadlock_count_.fetch_add(1, std::memory_order_relaxed);
                if (victim == txn->txn_id()) {
                    // Wait-die: this (younger) requester loses.
                    LOG_DEBUG("Deadlock: txn {} aborts itself (wait-die victim)",
                              txn->txn_id());
                    txn->set_aborted_by_deadlock();
                    txn->set_state(TransactionState::ABORTED);
                    erase_request_locked(queue, new_req_it, target);
                    return Status::Aborted("Deadlock detected (wait-die victim)");
                }
                // Wait-die: an older transaction wins over a younger holder in
                // the cycle. Abort the younger victim, freeing the lock we need.
                LOG_DEBUG("Deadlock: txn {} aborts younger holder {} (wait-die)",
                          txn->txn_id(), victim);
                if (Transaction* victim_txn = find_transaction_locked(victim)) {
                    abort_victim_locked(victim_txn);
                }
                // The victim's rollback releases its locks on its own thread;
                // the wait below returns as soon as our request is granted.
            }
        }

        if (queue->cv.wait_until(lock, deadline) == std::cv_status::timeout) {
            const bool aborted = !txn->is_active();
            // Timeout (or an abort racing the deadline) - remove our waiting
            // request and return.
            erase_request_locked(queue, new_req_it, target);
            if (aborted) {
                return Status::Aborted("Transaction was aborted");
            }
            LOG_DEBUG("Txn {} timed out waiting for lock", txn->txn_id());
            return Status::Timeout("Lock acquisition timed out");
        }

        // Aborted while waiting (selected as a deadlock victim by another
        // thread): withdraw our waiting request and let the caller's
        // TransactionManager::abort() undo-then-release.
        if (!txn->is_active()) {
            erase_request_locked(queue, new_req_it, target);
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
        // Remove waiting request; this can unblock requests FIFO-gated behind
        // it, so erase_request_locked re-evaluates grants.
        erase_request_locked(queue, req_it, target);
        return Status::Ok();
    }

    // Releasing a granted lock - transition to shrinking phase (2PL)
    if (txn->state() == TransactionState::GROWING) {
        txn->set_state(TransactionState::SHRINKING);
        LOG_DEBUG("Txn {} transitioning to SHRINKING phase", txn->txn_id());
    }

    // Remove the request and grant whatever became unblocked
    erase_request_locked(queue, req_it, target);

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
//
// Victim selection uses the WAIT-DIE policy. Transaction age is its id: a
// smaller txn_id is older and always wins. When a wait-for cycle is detected,
// the YOUNGEST transaction in the cycle (largest txn_id) is aborted. Because the
// oldest transaction in any cycle is never chosen, a transaction that keeps
// getting victimized eventually becomes the oldest live transaction and is then
// guaranteed to make progress — starvation (repeated victimization) is bounded.
// ─────────────────────────────────────────────────────────────────────────────

std::unordered_map<txn_id_t, std::vector<txn_id_t>>
LockManager::build_wait_for_graph() const {
    std::unordered_map<txn_id_t, std::vector<txn_id_t>> graph;

    // A holder blocks only while it is still active; a transaction already
    // marked ABORTED (a victim mid self-release) is about to drop its locks, so
    // excluding it prevents re-detecting the same, already-resolved cycle.
    const auto is_active_holder = [](const LockRequest& r) {
        return r.txn == nullptr || r.txn->is_active();
    };

    for (const auto& [target, q] : lock_table_) {
        for (const auto& req : q->request_queue) {
            // Skip aborted transactions entirely (neither block nor wait).
            if (req.txn != nullptr && !req.txn->is_active()) {
                continue;
            }

            std::vector<txn_id_t> blockers;

            if (!req.granted) {
                // A waiting request waits for every incompatible granted holder.
                for (const auto& other : q->request_queue) {
                    if (other.granted && other.txn_id != req.txn_id &&
                        is_active_holder(other) &&
                        !are_lock_modes_compatible(other.mode, req.mode)) {
                        blockers.push_back(other.txn_id);
                    }
                }
            } else if (q->upgrading && q->upgrading_txn_id == req.txn_id) {
                // An upgrading holder (has S, wants X) waits for every other
                // granted holder in the queue.
                for (const auto& other : q->request_queue) {
                    if (other.granted && other.txn_id != req.txn_id &&
                        is_active_holder(other)) {
                        blockers.push_back(other.txn_id);
                    }
                }
            }

            if (!blockers.empty()) {
                auto& edges = graph[req.txn_id];
                edges.insert(edges.end(), blockers.begin(), blockers.end());
            }
        }
    }

    return graph;
}

bool LockManager::find_cycle(
    txn_id_t node,
    const std::unordered_map<txn_id_t, std::vector<txn_id_t>>& graph,
    std::vector<txn_id_t>& path, std::unordered_set<txn_id_t>& in_path,
    std::unordered_set<txn_id_t>& visited,
    std::vector<txn_id_t>& cycle_out) const {

    visited.insert(node);
    path.push_back(node);
    in_path.insert(node);

    auto it = graph.find(node);
    if (it != graph.end()) {
        for (txn_id_t next : it->second) {
            if (in_path.count(next) > 0) {
                // Back-edge: the cycle is the path suffix starting at `next`.
                auto pos = std::find(path.begin(), path.end(), next);
                cycle_out.assign(pos, path.end());
                return true;
            }
            if (visited.count(next) == 0 &&
                find_cycle(next, graph, path, in_path, visited, cycle_out)) {
                return true;
            }
        }
    }

    path.pop_back();
    in_path.erase(node);
    return false;
}

txn_id_t LockManager::select_deadlock_victim(txn_id_t start_txn_id) const {
    auto graph = build_wait_for_graph();

    std::vector<txn_id_t> path;
    std::unordered_set<txn_id_t> in_path;
    std::unordered_set<txn_id_t> visited;
    std::vector<txn_id_t> cycle;

    if (!find_cycle(start_txn_id, graph, path, in_path, visited, cycle) ||
        cycle.empty()) {
        return INVALID_TXN_ID;
    }

    // Wait-die: youngest (largest id) transaction in the cycle is the victim.
    txn_id_t victim = cycle.front();
    for (txn_id_t id : cycle) {
        if (id > victim) {
            victim = id;
        }
    }
    return victim;
}

Transaction* LockManager::find_transaction_locked(txn_id_t txn_id) const {
    for (const auto& [target, q] : lock_table_) {
        for (const auto& req : q->request_queue) {
            if (req.txn_id == txn_id && req.txn != nullptr) {
                return req.txn;
            }
        }
    }
    return nullptr;
}

}  // namespace entropy
