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
  return is_created_visible(version, txn) && !is_delete_visible(version, txn);
}

bool MVCCManager::is_created_visible(const VersionInfo &version,
                                     const Transaction *txn) const {
  if (txn == nullptr) {
    // No transaction context: only committed creations are visible. A
    // rolled-back version has begin_ts == TIMESTAMP_MAX and is excluded.
    return version.begin_ts != 0 && version.begin_ts != TIMESTAMP_MAX;
  }

  const txn_id_t my_txn_id = txn->txn_id();

  // I created this version: visible to me unless it was rolled back
  // (rollback stamps begin_ts = TIMESTAMP_MAX).
  if (version.created_by == my_txn_id) {
    return version.begin_ts != TIMESTAMP_MAX;
  }

  // Created by another transaction: it must be committed (begin_ts stamped
  // with a real commit_ts) at or before my snapshot.
  if (version.begin_ts == 0 || version.begin_ts == TIMESTAMP_MAX) {
    return false; // uncommitted by another txn, or rolled back
  }
  return version.begin_ts <= txn->start_ts();
}

bool MVCCManager::is_delete_visible(const VersionInfo &version,
                                    const Transaction *txn) const {
  if (!version.is_deleted()) {
    return false;
  }

  if (txn == nullptr) {
    // Without a snapshot, any delete (committed or not) hides the version,
    // matching "latest committed, undeleted" semantics.
    return true;
  }

  // I deleted it: not visible to me.
  if (version.deleted_by == txn->txn_id()) {
    return true;
  }

  // Deleted by another transaction: the delete only hides the version once it
  // is committed (end_ts stamped) at or before my snapshot.
  if (version.end_ts == TIMESTAMP_MAX) {
    return false; // delete not yet committed
  }
  return version.end_ts <= txn->start_ts();
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

void MVCCManager::finalize_commit(VersionInfo &version, const Transaction *txn,
                                  uint64_t commit_ts) {
  const txn_id_t my_txn_id = txn ? txn->txn_id() : TXN_ID_NONE;
  if (my_txn_id == TXN_ID_NONE) {
    return; // nothing to attribute to a null/none transaction
  }

  // If this transaction created the version, stamp its begin_ts.
  if (version.created_by == my_txn_id && version.begin_ts == 0) {
    version.begin_ts = commit_ts;
  }
  // If this transaction deleted the version, stamp its end_ts. This is
  // independent of the creation stamp, so committing a DELETE never touches a
  // version the transaction merely created and vice versa.
  if (version.deleted_by == my_txn_id && version.end_ts == TIMESTAMP_MAX) {
    version.end_ts = commit_ts;
  }
}

void MVCCManager::rollback_version(VersionInfo &version,
                                   const Transaction *txn) {
  const txn_id_t my_txn_id = txn ? txn->txn_id() : TXN_ID_NONE;

  // Case 1: this transaction only deleted a version it did not create. Undo
  // the delete so the underlying committed version remains visible. Without
  // this, aborting a DELETE would erase a committed row (#27).
  if (my_txn_id != TXN_ID_NONE && version.deleted_by == my_txn_id &&
      version.created_by != my_txn_id) {
    version.deleted_by = TXN_ID_NONE;
    version.end_ts = TIMESTAMP_MAX;
    return;
  }

  // Case 2: this transaction created the version (whether or not it also
  // deleted it). The version never committed, so make it invisible to all.
  version.begin_ts = TIMESTAMP_MAX;
  version.end_ts = 0;
}

} // namespace entropy
