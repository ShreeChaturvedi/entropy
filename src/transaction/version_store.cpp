/**
 * @file version_store.cpp
 * @brief In-memory MVCC version chains keyed by RID
 */

#include "transaction/version_store.hpp"

#include <cstddef>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "transaction/transaction.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Write path
// ─────────────────────────────────────────────────────────────────────────────

Status VersionStore::on_insert(const Transaction *txn, RID rid) {
  if (txn == nullptr) {
    return Status::InvalidArgument("on_insert requires a transaction");
  }

  std::unique_lock lock(latch_);
  VersionNode node;
  mvcc_.init_version(node.info, txn); // created_by = txn, begin_ts = 0
  chains_[rid].push_back(std::move(node));
  txn_touched_[txn->txn_id()].insert(rid);
  return Status::Ok();
}

Status VersionStore::on_update(const Transaction *txn, RID rid,
                               std::span<const char> before_image) {
  if (txn == nullptr) {
    return Status::InvalidArgument("on_update requires a transaction");
  }

  std::unique_lock lock(latch_);
  auto &chain = chains_[rid];
  if (chain.empty()) {
    // Row predates its chain (e.g. loaded from disk): materialize a committed
    // base version. Its bytes are the current heap content.
    VersionNode base;
    base.info.created_by = TXN_ID_NONE;
    base.info.deleted_by = TXN_ID_NONE;
    base.info.begin_ts = TIMESTAMP_PREHISTORY; // committed before any live txn
    base.info.end_ts = TIMESTAMP_MAX;
    chain.push_back(std::move(base));
  }

  const txn_id_t tid = txn->txn_id();
  if (has_write_conflict(chain.back().info, txn)) {
    return Status::Aborted("write-write conflict on update");
  }

  if (chain.back().info.created_by == tid && chain.back().info.begin_ts == 0) {
    // Already my own uncommitted version: the heap holds the newest bytes and
    // the pre-transaction before-image is already retained. Nothing to push.
    txn_touched_[tid].insert(rid);
    return Status::Ok();
  }

  // Preserve the superseded version's bytes, then push a new uncommitted head.
  chain.back().before_image.assign(before_image.begin(), before_image.end());
  VersionNode head;
  mvcc_.init_version(head.info, txn); // created_by = txn, begin_ts = 0
  chain.push_back(std::move(head));
  txn_touched_[tid].insert(rid);
  return Status::Ok();
}

Status VersionStore::on_delete(const Transaction *txn, RID rid,
                               std::span<const char> before_image) {
  if (txn == nullptr) {
    return Status::InvalidArgument("on_delete requires a transaction");
  }

  std::unique_lock lock(latch_);
  auto &chain = chains_[rid];
  if (chain.empty()) {
    VersionNode base;
    base.info.created_by = TXN_ID_NONE;
    base.info.deleted_by = TXN_ID_NONE;
    base.info.begin_ts = TIMESTAMP_PREHISTORY;
    base.info.end_ts = TIMESTAMP_MAX;
    chain.push_back(std::move(base));
  }

  const txn_id_t tid = txn->txn_id();
  if (has_write_conflict(chain.back().info, txn)) {
    return Status::Aborted("write-write conflict on delete");
  }

  VersionNode &cur = chain.back();
  if (cur.info.deleted_by == tid) {
    return Status::Ok(); // already deleted by me (idempotent)
  }

  mvcc_.mark_deleted(cur.info, txn); // deleted_by = txn, end_ts stamped at commit
  // Retain the deleted bytes so snapshots that do not yet see the delete can
  // still read the row after the heap slot is reclaimed.
  cur.before_image.assign(before_image.begin(), before_image.end());
  txn_touched_[tid].insert(rid);
  return Status::Ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Read path
// ─────────────────────────────────────────────────────────────────────────────

std::optional<std::vector<char>>
VersionStore::read_visible(RID rid, std::span<const char> heap_bytes,
                           const Transaction *txn) const {
  std::shared_lock lock(latch_);
  auto it = chains_.find(rid);
  if (it == chains_.end()) {
    // No MVCC metadata: treat the heap value as a committed, visible row.
    return std::vector<char>(heap_bytes.begin(), heap_bytes.end());
  }

  const auto &chain = it->second;
  for (auto rit = chain.rbegin(); rit != chain.rend(); ++rit) {
    const VersionNode &node = *rit;
    if (!mvcc_.is_created_visible(node.info, txn)) {
      continue; // created after my snapshot: consider an older version
    }
    if (mvcc_.is_delete_visible(node.info, txn)) {
      return std::nullopt; // visible creation but deleted from my view: gone
    }
    // This is the version I see. Return an owned copy: the shared lock is
    // released on return, so a span into before_image would dangle against a
    // concurrent gc()/rollback().
    const bool is_head = (rit == chain.rbegin());
    if (is_head && node.info.deleted_by == TXN_ID_NONE) {
      return std::vector<char>(heap_bytes.begin(),
                               heap_bytes.end()); // live head: heap bytes
    }
    return node.before_image; // superseded/deleted version: retained copy
  }
  return std::nullopt;
}

// ─────────────────────────────────────────────────────────────────────────────
// Commit / abort / GC
// ─────────────────────────────────────────────────────────────────────────────

void VersionStore::finalize(const Transaction *txn, uint64_t commit_ts) {
  if (txn == nullptr) {
    return;
  }

  std::unique_lock lock(latch_);
  auto tit = txn_touched_.find(txn->txn_id());
  if (tit == txn_touched_.end()) {
    return;
  }

  for (const RID &rid : tit->second) {
    auto cit = chains_.find(rid);
    if (cit == chains_.end()) {
      continue;
    }
    for (VersionNode &node : cit->second) {
      mvcc_.finalize_commit(node.info, txn, commit_ts);
    }
  }
  txn_touched_.erase(tit);
}

void VersionStore::rollback(const Transaction *txn) {
  if (txn == nullptr) {
    return;
  }

  std::unique_lock lock(latch_);
  auto tit = txn_touched_.find(txn->txn_id());
  if (tit == txn_touched_.end()) {
    return;
  }

  const txn_id_t tid = txn->txn_id();
  for (const RID &rid : tit->second) {
    auto cit = chains_.find(rid);
    if (cit == chains_.end()) {
      continue;
    }
    auto &chain = cit->second;

    // Walk newest-first: remove uncommitted creations, revert uncommitted
    // deletes. At most one node per chain is uncommitted by this transaction.
    for (size_t i = chain.size(); i-- > 0;) {
      VersionInfo &info = chain[i].info;
      const bool created_here =
          (info.created_by == tid && info.begin_ts == 0);
      const bool deleted_here =
          (info.deleted_by == tid && info.end_ts == TIMESTAMP_MAX);
      if (created_here) {
        // Uncommitted creation (covers create and create+delete): drop it. If
        // this was an update, the superseded version below becomes the head.
        chain.erase(chain.begin() + static_cast<std::ptrdiff_t>(i));
      } else if (deleted_here) {
        // Uncommitted delete of a version this transaction did not create:
        // undo just the delete; the committed version survives.
        mvcc_.rollback_version(info, txn);
        chain[i].before_image.clear();
      }
    }

    if (chain.empty()) {
      chains_.erase(cit);
    }
  }
  txn_touched_.erase(tit);
}

void VersionStore::gc(uint64_t min_active_start_ts) {
  std::unique_lock lock(latch_);
  for (auto it = chains_.begin(); it != chains_.end();) {
    auto &chain = it->second;
    if (chain.empty()) {
      it = chains_.erase(it);
      continue;
    }

    // 1. Find the newest committed version visible to the oldest snapshot;
    //    everything strictly older than it is unreachable and can be pruned.
    size_t keep_from = 0;
    for (size_t i = chain.size(); i-- > 0;) {
      const VersionInfo &info = chain[i].info;
      if (is_committed_create(info) && info.begin_ts <= min_active_start_ts) {
        keep_from = i;
        break;
      }
    }
    if (keep_from > 0) {
      chain.erase(chain.begin(),
                  chain.begin() + static_cast<std::ptrdiff_t>(keep_from));
    }

    // 2. If the row is committed-deleted and the delete is visible to every
    //    active snapshot, no one can observe it: drop the whole chain.
    const VersionInfo &head = chain.back().info;
    if (is_committed_delete(head) && head.end_ts <= min_active_start_ts) {
      it = chains_.erase(it);
      continue;
    }
    ++it;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Introspection
// ─────────────────────────────────────────────────────────────────────────────

size_t VersionStore::chain_count() const {
  std::shared_lock lock(latch_);
  return chains_.size();
}

size_t VersionStore::chain_length(RID rid) const {
  std::shared_lock lock(latch_);
  auto it = chains_.find(rid);
  return it == chains_.end() ? 0 : it->second.size();
}

// ─────────────────────────────────────────────────────────────────────────────
// Conflict detection (first-updater-wins)
// ─────────────────────────────────────────────────────────────────────────────

bool VersionStore::has_write_conflict(const VersionInfo &cur,
                                      const Transaction *txn) const {
  const txn_id_t tid = txn->txn_id();
  const uint64_t start_ts = txn->start_ts();

  // I already own the current change: no conflict with myself.
  if (cur.created_by == tid && cur.begin_ts == 0) {
    return false;
  }
  if (cur.deleted_by == tid) {
    return false;
  }

  // Another transaction has an uncommitted creation here.
  if (cur.begin_ts == 0 && cur.created_by != tid) {
    return true;
  }
  // Another transaction has an uncommitted deletion here.
  if (cur.deleted_by != TXN_ID_NONE && cur.deleted_by != tid &&
      cur.end_ts == TIMESTAMP_MAX) {
    return true;
  }
  // Another transaction committed a delete of this row (in any snapshot
  // order): the row I would write no longer exists as I saw it.
  if (is_committed_delete(cur)) {
    return true;
  }
  // A committed creation newer than my snapshot: writing would silently lose
  // an update I cannot see.
  if (is_committed_create(cur) && cur.begin_ts > start_ts) {
    return true;
  }
  return false;
}

} // namespace entropy
