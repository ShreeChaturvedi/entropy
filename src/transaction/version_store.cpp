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
namespace {

// A row loaded from disk predates its version chain. Materialize a synthetic
// committed base version whose bytes are the current heap content: committed at
// pre-history so it is visible to every snapshot, and never deleted.
VersionStore::VersionNode make_prehistory_base() {
  VersionStore::VersionNode base;
  base.info.created_by = TXN_ID_NONE;
  base.info.deleted_by = TXN_ID_NONE;
  base.info.begin_ts = TIMESTAMP_PREHISTORY;
  base.info.end_ts = TIMESTAMP_MAX;
  return base;
}

/// A version invalidated by rollback (begin_ts == TIMESTAMP_MAX). Kept on the
/// chain as a tombstone so a reader that raced the abort's two-step teardown
/// (heap bytes removed, then chain updated) still finds a marker proving the
/// bytes it copied were never committed. Invisible to every snapshot forever.
bool is_invalidated(const VersionInfo &info) noexcept {
  return info.begin_ts == TIMESTAMP_MAX;
}

/// Drop trailing rollback tombstones so mutators and GC always operate on the
/// last real version. Safe at any time: a tombstone's heap bytes were removed
/// before the tombstone was created, so nothing can resurrect through it.
void prune_invalidated_tail(std::vector<VersionStore::VersionNode> &chain) {
  while (!chain.empty() && is_invalidated(chain.back().info)) {
    chain.pop_back();
  }
}

} // namespace

// ─────────────────────────────────────────────────────────────────────────────
// Write path
// ─────────────────────────────────────────────────────────────────────────────

Status VersionStore::on_insert(const Transaction *txn, RID rid) {
  if (txn == nullptr) {
    return Status::InvalidArgument("on_insert requires a transaction");
  }

  std::unique_lock lock(latch_);
  auto &chain = chains_[rid];
  prune_invalidated_tail(chain);
  VersionNode node;
  mvcc_.init_version(node.info, txn); // created_by = txn, begin_ts = 0
  chain.push_back(std::move(node));
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
  prune_invalidated_tail(chain);
  if (chain.empty()) {
    chain.push_back(make_prehistory_base());
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
  prune_invalidated_tail(chain);
  if (chain.empty()) {
    chain.push_back(make_prehistory_base());
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
    // No MVCC metadata. An occupied slot is a committed, visible row (data
    // that predates its chain, e.g. loaded from disk). An empty slot with no
    // chain is simply a hole — nothing to see.
    if (heap_bytes.empty()) {
      return std::nullopt;
    }
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
      if (heap_bytes.empty()) {
        // A live head's bytes should be in the heap; an empty slot here means
        // the reader raced a teardown mid-step. Nothing sound to return.
        return std::nullopt;
      }
      return std::vector<char>(heap_bytes.begin(),
                               heap_bytes.end()); // live head: heap bytes
    }
    return node.before_image; // superseded/deleted version: retained copy
  }
  return std::nullopt;
}

bool VersionStore::has_pending_delete(RID rid) const {
  std::shared_lock lock(latch_);
  auto it = chains_.find(rid);
  if (it == chains_.end()) {
    return false;
  }
  const auto &chain = it->second;
  // The current version is the newest node that is not a rollback tombstone.
  for (auto rit = chain.rbegin(); rit != chain.rend(); ++rit) {
    if (is_invalidated(rit->info)) {
      continue;
    }
    // Uncommitted delete: a deleter is stamped but its end_ts has not been
    // finalized to a real commit timestamp yet.
    return rit->info.deleted_by != TXN_ID_NONE &&
           rit->info.end_ts == TIMESTAMP_MAX;
  }
  return false;
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

    // Walk newest-first: invalidate uncommitted creations, revert uncommitted
    // deletes. At most one node per chain is uncommitted by this transaction.
    for (size_t i = chain.size(); i-- > 0;) {
      VersionInfo &info = chain[i].info;
      const bool created_here =
          (info.created_by == tid && info.begin_ts == 0);
      const bool deleted_here =
          (info.deleted_by == tid && info.end_ts == TIMESTAMP_MAX);
      if (created_here || deleted_here) {
        // rollback_version invalidates a created version (begin_ts = MAX) or
        // reverts a bare delete. An invalidated creation is KEPT as a
        // tombstone rather than erased: the abort's teardown is two steps
        // (heap bytes removed, then the chain updated), and a reader that
        // copied the bytes before step one must still find a chain entry
        // proving they were never committed when it checks visibility after
        // step two. The next writer on this RID prunes tombstones; GC prunes
        // them once no snapshot old enough to have copied the doomed bytes
        // is still active, using the invalidation timestamp stamped here
        // into the (otherwise meaningless) end_ts.
        mvcc_.rollback_version(info, txn);
        if (info.begin_ts == TIMESTAMP_MAX) {
          info.end_ts = mvcc_.get_timestamp();
        }
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
    // Rollback tombstones are invisible to everyone, but they may only be
    // ERASED once every transaction old enough to have copied the aborted
    // bytes (before the abort's undo removed them) has ended — a younger
    // reader consulting an already-erased chain would take stale bytes for a
    // committed row. The invalidation time lives in the tombstone's end_ts.
    // (Mutators prune unconditionally: they replace the tombstone with a
    // real head, so the chain keeps marking the RID.)
    while (!chain.empty() && is_invalidated(chain.back().info) &&
           chain.back().info.end_ts < min_active_start_ts) {
      chain.pop_back();
    }
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
    //    active snapshot, no one can observe it: drop the whole chain. A
    //    surviving tombstone head is NOT a committed delete even though its
    //    end_ts is stamped — skip such chains until the tombstone retires.
    const VersionInfo &head = chain.back().info;
    if (!is_invalidated(head) && is_committed_delete(head) &&
        head.end_ts <= min_active_start_ts) {
      it = chains_.erase(it);
      continue;
    }
    ++it;
  }
}

void VersionStore::purge_pages(const std::unordered_set<page_id_t> &pages) {
  if (pages.empty()) {
    return;
  }
  std::unique_lock lock(latch_);
  for (auto it = chains_.begin(); it != chains_.end();) {
    if (pages.contains(it->first.page_id)) {
      it = chains_.erase(it);
    } else {
      ++it;
    }
  }
  // txn_touched_ entries pointing at purged RIDs are harmless: finalize and
  // rollback skip RIDs whose chains no longer exist.
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
