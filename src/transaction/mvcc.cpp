/**
 * @file mvcc.cpp
 * @brief MVCC implementation
 */

#include "transaction/mvcc.hpp"

#include "common/logger.hpp"
#include "transaction/transaction.hpp"

namespace entropy {

MVCCManager::MVCCManager() = default;

uint64_t MVCCManager::get_timestamp() noexcept {
  return global_timestamp_.fetch_add(1);
}

bool MVCCManager::is_visible(const VersionInfo &version,
                             const Transaction *txn) const {
  if (txn == nullptr) {
    // No transaction context - treat as seeing latest committed versions
    return version.begin_ts != 0 && !version.is_deleted();
  }

  txn_id_t my_txn_id = txn->txn_id();
  uint64_t my_start_ts = txn->start_ts();

  // Rule 1: If I created this version, it's visible to me
  if (version.created_by == my_txn_id) {
    // But not if I also deleted it
    return version.deleted_by != my_txn_id;
  }

  // Rule 2: Version must be committed before my start
  // (begin_ts is set to creator's commit_ts when they commit)
  if (version.begin_ts == 0 || version.begin_ts > my_start_ts) {
    // Version was created by an uncommitted transaction (begin_ts == 0)
    // or was committed after I started
    return false;
  }

  // Rule 3: Check if version is deleted
  if (version.deleted_by == TXN_ID_NONE) {
    // Not deleted - visible
    return true;
  }

  // Version was deleted - check if the delete is visible to me
  if (version.deleted_by == my_txn_id) {
    // I deleted it - not visible
    return false;
  }

  // Deleted by another transaction - visible only if delete not yet committed
  // or committed after my start
  if (version.end_ts == TIMESTAMP_MAX) {
    // Deleter hasn't committed yet - version still visible to me
    return true;
  }

  // end_ts is the deleter's commit timestamp
  return version.end_ts > my_start_ts;
}

bool MVCCManager::is_visible_read_committed(
    const VersionInfo &version, const Transaction *txn,
    std::function<bool(txn_id_t)> is_committed) const {

  if (txn == nullptr) {
    return version.begin_ts != 0 && !version.is_deleted();
  }

  txn_id_t my_txn_id = txn->txn_id();

  // Rule 1: If I created this version, it's visible to me
  if (version.created_by == my_txn_id) {
    return version.deleted_by != my_txn_id;
  }

  // Rule 2: Version must be created by a committed transaction
  if (version.begin_ts == 0) {
    // Creator hasn't committed - check if they have now
    if (!is_committed(version.created_by)) {
      return false;
    }
  }

  // Rule 3: Check if version is deleted
  if (version.deleted_by == TXN_ID_NONE) {
    return true;
  }

  if (version.deleted_by == my_txn_id) {
    return false;
  }

  // For read committed, deleted version is invisible only if deleter committed
  if (version.end_ts != TIMESTAMP_MAX) {
    // Deleter committed - version invisible
    return false;
  }

  // Deleter not committed yet - check if they have now
  return !is_committed(version.deleted_by);
}

void MVCCManager::init_version(VersionInfo &version, const Transaction *txn) {
  version.created_by = txn ? txn->txn_id() : TXN_ID_NONE;
  version.deleted_by = TXN_ID_NONE;
  version.begin_ts = 0; // Set to commit_ts when transaction commits
  version.end_ts = TIMESTAMP_MAX;
}

void MVCCManager::mark_deleted(VersionInfo &version, const Transaction *txn) {
  version.deleted_by = txn ? txn->txn_id() : TXN_ID_NONE;
  version.end_ts = TIMESTAMP_MAX; // Set to commit_ts when deleter commits
}

void MVCCManager::finalize_commit(VersionInfo &version, uint64_t commit_ts) {
  // If this transaction created the version, set begin_ts
  if (version.begin_ts == 0) {
    version.begin_ts = commit_ts;
  }
  // If this transaction deleted the version, set end_ts
  if (version.end_ts == TIMESTAMP_MAX && version.deleted_by != TXN_ID_NONE) {
    version.end_ts = commit_ts;
  }
}

void MVCCManager::rollback_version(VersionInfo &version) {
  // Mark version as invalid by setting begin_ts to MAX
  // This ensures no transaction will see it
  version.begin_ts = TIMESTAMP_MAX;
  version.end_ts = 0;
}

} // namespace entropy
