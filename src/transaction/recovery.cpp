/**
 * @file recovery.cpp
 * @brief ARIES-lite crash recovery: checkpoint-anchored redo with page-LSN
 *        gating, plus physical undo of loser transactions.
 */

#include "transaction/recovery.hpp"

#include <algorithm>
#include <cstring>

#include "common/logger.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/disk_manager.hpp"
#include "storage/page.hpp"
#include "storage/table_page.hpp"
#include "transaction/wal.hpp"

namespace entropy {

namespace {

/// Convert a log record's stored bytes to a uint16_t record size, clamping to
/// the slotted-page limit. Recovery only replays records the engine itself
/// wrote, so this is a defensive guard, not a functional path.
[[nodiscard]] bool to_record_size(size_t n, uint16_t *out) {
  if (n == 0 || n > Page::kPageSize) {
    return false;
  }
  *out = static_cast<uint16_t>(n);
  return true;
}

/// Whether the record describes a page mutation (redo/undo candidate).
[[nodiscard]] bool is_data_record(LogRecordType type) {
  return type == LogRecordType::INSERT || type == LogRecordType::DELETE ||
         type == LogRecordType::UPDATE;
}

/// True when the slot currently holds `image`. Prefix comparison: an in-place
/// shrink keeps the slot's original length, so the stored record may carry
/// stale trailing bytes beyond the logged image.
[[nodiscard]] bool slot_holds(const TablePage &page, slot_id_t slot_id,
                              const std::vector<char> &image) {
  std::span<const char> rec = page.get_record(slot_id);
  return !rec.empty() && !image.empty() && rec.size() >= image.size() &&
         std::memcmp(rec.data(), image.data(), image.size()) == 0;
}

/// True when the slot's stored record is byte-for-byte exactly `image`,
/// length included. Undo-INSERT uses this so a longer committed row that
/// merely shares a prefix with the loser's insert is never mistaken for it.
[[nodiscard]] bool slot_holds_exactly(const TablePage &page, slot_id_t slot_id,
                                      const std::vector<char> &image) {
  std::span<const char> rec = page.get_record(slot_id);
  return !image.empty() && rec.size() == image.size() &&
         std::memcmp(rec.data(), image.data(), image.size()) == 0;
}

} // namespace

RecoveryManager::RecoveryManager(std::shared_ptr<BufferPoolManager> buffer_pool,
                                 std::shared_ptr<WALManager> wal,
                                 std::shared_ptr<DiskManager> disk_manager)
    : buffer_pool_(std::move(buffer_pool)), wal_(std::move(wal)),
      disk_manager_(std::move(disk_manager)) {}

Status RecoveryManager::recover() {
  LOG_INFO("Starting crash recovery...");

  if (!wal_) {
    LOG_WARN("No WAL manager provided, skipping recovery");
    return Status::Ok();
  }

  std::vector<LogRecord> records = wal_->read_log();

  // Reset per-run state.
  active_txn_table_.clear();
  committed_txns_.clear();
  redo_start_lsn_ = INVALID_LSN;
  recovered_next_txn_id_ = 1;
  redo_count_ = 0;
  undo_count_ = 0;

  if (records.empty()) {
    LOG_INFO("WAL is empty, nothing to recover");
    return Status::Ok();
  }

  LOG_INFO("Read {} log records from WAL", records.size());

  // Phase 1: Analysis
  LOG_INFO("=== ANALYSIS PHASE ===");
  analysis_phase(records);
  LOG_INFO("Analysis complete: {} active txns, {} committed txns, "
           "redo_start_lsn={}, next_txn_id={}",
           active_txn_table_.size(), committed_txns_.size(), redo_start_lsn_,
           recovered_next_txn_id_);

  // Phase 2: Redo
  LOG_INFO("=== REDO PHASE ===");
  Status redo_status = redo_phase(records);
  if (!redo_status.ok()) {
    LOG_ERROR("Redo phase failed: {}", redo_status.message());
    return redo_status;
  }
  LOG_INFO("Redo complete: {} operations applied", redo_count_);

  // Phase 3: Undo
  LOG_INFO("=== UNDO PHASE ===");
  Status undo_status = undo_phase(records);
  if (!undo_status.ok()) {
    LOG_ERROR("Undo phase failed: {}", undo_status.message());
    return undo_status;
  }
  LOG_INFO("Undo complete: {} operations rolled back", undo_count_);

  LOG_INFO("Recovery complete!");
  return Status::Ok();
}

Status RecoveryManager::create_checkpoint(
    const std::vector<txn_id_t> &active_txn_ids,
    std::shared_mutex *write_barrier) {
  if (!wal_) {
    return Status::InvalidArgument("No WAL manager provided");
  }

  // The redo anchor is only safe when every record older than it is durably
  // reflected on disk pages. Capture the anchor candidate first, then flush
  // the WAL (so all pre-anchor records are durable for analysis/undo), then
  // flush all dirty pages (so all pre-anchor effects are durable). Without a
  // buffer pool no pages can be flushed, so no anchor is recorded and
  // recovery falls back to a full page-LSN-gated scan.
  //
  // Quiesce writers for the capture+flush window: with the barrier held
  // exclusively, no logging writer can be mid-append (they take it shared
  // across append + page-LSN stamp), so every record with LSN < begin_lsn has
  // already stamped and dirtied its page and is captured by flush_all_pages.
  // A null barrier keeps the historical single-threaded contract.
  auto do_capture_and_flush = [&](lsn_t &begin_lsn) -> Status {
    if (!buffer_pool_) {
      return Status::Ok();
    }
    begin_lsn = wal_->next_lsn();
    Status flush_status = wal_->flush();
    if (!flush_status.ok()) {
      return flush_status;
    }
    // The anchor is a promise that every pre-anchor effect is on disk; a page
    // that failed to flush breaks that promise, so surface the error rather
    // than record an unsound anchor.
    return buffer_pool_->flush_all_pages();
  };

  lsn_t begin_lsn = INVALID_LSN;
  if (write_barrier != nullptr) {
    std::unique_lock<std::shared_mutex> quiesce(*write_barrier);
    Status s = do_capture_and_flush(begin_lsn);
    if (!s.ok()) {
      return s;
    }
  } else {
    Status s = do_capture_and_flush(begin_lsn);
    if (!s.ok()) {
      return s;
    }
  }

  LogRecord checkpoint = LogRecord::make_checkpoint(active_txn_ids, begin_lsn);
  if (wal_->append_log(checkpoint) == INVALID_LSN) {
    return Status::IOError("Failed to append CHECKPOINT record");
  }

  return wal_->flush();
}

// ─────────────────────────────────────────────────────────────────────────────
// Analysis Phase
// ─────────────────────────────────────────────────────────────────────────────

void RecoveryManager::analysis_phase(const std::vector<LogRecord> &records) {
  // Locate the last checkpoint. It supplies the redo anchor (begin_lsn) and a
  // high-water next_txn_id, and seeds the set of transactions known to be
  // active at the checkpoint moment.
  const LogRecord *checkpoint = nullptr;
  for (size_t i = records.size(); i > 0; --i) {
    if (records[i - 1].type() == LogRecordType::CHECKPOINT) {
      checkpoint = &records[i - 1];
      break;
    }
  }

  if (checkpoint != nullptr) {
    // A valid begin_lsn anchors redo; without one, redo scans from the start
    // and relies purely on page-LSN gating (always correct, just less optimal).
    if (checkpoint->begin_lsn() != INVALID_LSN) {
      redo_start_lsn_ = checkpoint->begin_lsn();
    }
    if (checkpoint->next_txn_id() != INVALID_TXN_ID) {
      recovered_next_txn_id_ =
          std::max(recovered_next_txn_id_, checkpoint->next_txn_id());
    }
    for (txn_id_t txn_id : checkpoint->active_txns()) {
      active_txn_table_[txn_id] = INVALID_LSN;
    }
  }

  // Scan the (in-memory) log to reconstruct the exact set of loser transactions
  // and the committed set, and to find the highest transaction id observed.
  txn_id_t max_txn_id = 0;
  for (const LogRecord &record : records) {
    const txn_id_t txn_id = record.txn_id();

    switch (record.type()) {
    case LogRecordType::BEGIN:
      active_txn_table_[txn_id] = record.lsn();
      break;

    case LogRecordType::COMMIT:
      active_txn_table_.erase(txn_id);
      committed_txns_.insert(txn_id);
      break;

    case LogRecordType::ABORT:
      // Already rolled back during normal operation (or a prior recovery).
      active_txn_table_.erase(txn_id);
      break;

    case LogRecordType::INSERT:
    case LogRecordType::DELETE:
    case LogRecordType::UPDATE:
      active_txn_table_[txn_id] = record.lsn();
      break;

    default:
      break;
    }

    if (txn_id != INVALID_TXN_ID) {
      max_txn_id = std::max(max_txn_id, txn_id);
    }
  }

  recovered_next_txn_id_ = std::max(recovered_next_txn_id_, max_txn_id + 1);

  LOG_DEBUG("After analysis: {} uncommitted transactions need undo",
            active_txn_table_.size());
}

// ─────────────────────────────────────────────────────────────────────────────
// Redo Phase
// ─────────────────────────────────────────────────────────────────────────────

Status RecoveryManager::redo_phase(const std::vector<LogRecord> &records) {
  if (!buffer_pool_) {
    // Analysis-only mode (no page store wired): nothing to replay.
    return Status::Ok();
  }

  // Repeat history from the redo anchor forward. Page-LSN gating in
  // redo_record makes each application idempotent.
  for (const LogRecord &record : records) {
    if (record.lsn() < redo_start_lsn_ || !is_data_record(record.type())) {
      continue;
    }

    Status status = redo_record(record);
    if (!status.ok()) {
      LOG_WARN("Redo failed for LSN {}: {}", record.lsn(), status.message());
    }
  }

  return Status::Ok();
}

Status RecoveryManager::redo_record(const LogRecord &record) {
  const RID rid = record.rid();
  if (rid.page_id < 0) {
    return Status::InvalidArgument("Redo record has invalid page id");
  }

  Page *page = buffer_pool_->fetch_page(rid.page_id);
  if (page == nullptr) {
    return Status::NotFound("Page not available for redo");
  }

  // Page-LSN gate: skip if this change is already reflected on the page.
  if (page->lsn() >= record.lsn()) {
    buffer_pool_->unpin_page(rid.page_id, false);
    return Status::Ok();
  }

  // A page that has never held table data (freshly created or never flushed)
  // must be initialized before we can place records into it.
  TablePage table_page(page);
  if (page->page_type() != PageType::TABLE_PAGE) {
    table_page.init();
  }

  bool applied = false;
  switch (record.type()) {
  case LogRecordType::INSERT: {
    uint16_t size = 0;
    if (!to_record_size(record.new_tuple_data().size(), &size)) {
      break;
    }
    const char *data = record.new_tuple_data().data();
    const slot_id_t slot = rid.slot_id;
    const uint16_t slot_count = table_page.get_slot_count();
    if (slot < slot_count) {
      // Reused (previously deleted) slot: place the record back at that slot.
      applied = table_page.insert_record_at(slot, data, size);
    } else if (slot == slot_count) {
      // Append: repeating history from the matching page state reproduces the
      // same slot id the original insert obtained.
      auto placed = table_page.insert_record(data, size);
      applied = placed.has_value() && *placed == slot;
    }
    break;
  }
  case LogRecordType::UPDATE: {
    uint16_t size = 0;
    if (!to_record_size(record.new_tuple_data().size(), &size)) {
      break;
    }
    applied =
        table_page.update_record(rid.slot_id, record.new_tuple_data().data(), size);
    break;
  }
  case LogRecordType::DELETE: {
    applied = table_page.delete_record(rid.slot_id);
    break;
  }
  default:
    break;
  }

  if (applied) {
    page->set_lsn(record.lsn());
    ++redo_count_;
  } else {
    LOG_WARN("Redo could not apply {} at ({},{}) LSN {}",
             log_record_type_to_string(record.type()), rid.page_id, rid.slot_id,
             record.lsn());
  }

  buffer_pool_->unpin_page(rid.page_id, applied);
  return Status::Ok();
}

// ─────────────────────────────────────────────────────────────────────────────
// Undo Phase
// ─────────────────────────────────────────────────────────────────────────────

Status RecoveryManager::undo_phase(const std::vector<LogRecord> &records) {
  if (active_txn_table_.empty()) {
    LOG_DEBUG("No uncommitted transactions to undo");
    return Status::Ok();
  }
  if (!buffer_pool_) {
    // Cannot physically undo without a page store; leave the ATT as reported.
    return Status::Ok();
  }

  // Undo every loser operation in one merged scan in global reverse-LSN order
  // (not per-transaction chains), so interleaved losers unwind in the exact
  // inverse of the order their effects were applied.
  std::unordered_set<page_id_t> loser_pages;
  for (auto it = records.rbegin(); it != records.rend(); ++it) {
    const LogRecord &record = *it;
    if (!is_data_record(record.type()) ||
        active_txn_table_.count(record.txn_id()) == 0) {
      continue;
    }

    // Track EVERY examined loser page, not only pages mutated in this run: a
    // prior recover() attempt may have applied a compensation in-pool and then
    // failed the flush; on retry the state-checked undo skips the op, but that
    // compensation still sits only dirty in the pool and must reach disk
    // before any ABORT becomes durable.
    if (record.rid().page_id >= 0) {
      loser_pages.insert(record.rid().page_id);
    }

    Status status = undo_record(record);
    if (!status.ok()) {
      LOG_WARN("Undo failed for LSN {}: {}", record.lsn(), status.message());
    }
  }

  // Durability ordering: every compensation must be on disk BEFORE any ABORT
  // becomes durable. Once an ABORT is durable, a later recovery no longer
  // treats the transaction as a loser, so an unflushed compensation would be
  // lost forever if we crashed here with the ABORT already in the log.
  for (page_id_t page_id : loser_pages) {
    // fetch_page pins the page (re-reading it if it was evicted, in which case
    // its state is already on disk), so a false from flush_page can only mean
    // a real write failure.
    Page *page = buffer_pool_->fetch_page(page_id);
    if (page == nullptr) {
      return Status::IOError("Failed to pin undone page for flush");
    }
    const bool flushed = buffer_pool_->flush_page(page_id);
    buffer_pool_->unpin_page(page_id, false);
    if (!flushed) {
      return Status::IOError("Failed to flush undone page before ABORT");
    }
  }

  // Log an ABORT per loser so a subsequent recovery treats it as already
  // rolled back; state-checked undo keeps a re-run (crash before this flush)
  // harmless.
  for (const auto &[txn_id, last_lsn] : active_txn_table_) {
    LogRecord abort_record = LogRecord::make_abort(txn_id, last_lsn);
    if (wal_->append_log(abort_record) == INVALID_LSN) {
      return Status::IOError("Failed to append ABORT record during recovery");
    }
  }
  return wal_->flush();
}

Status RecoveryManager::undo_record(const LogRecord &record) {
  const RID rid = record.rid();
  if (rid.page_id < 0) {
    return Status::InvalidArgument("Undo record has invalid page id");
  }

  Page *page = buffer_pool_->fetch_page(rid.page_id);
  if (page == nullptr) {
    return Status::NotFound("Page not available for undo");
  }

  TablePage table_page(page);
  bool applied = false;

  switch (record.type()) {
  case LogRecordType::INSERT:
    // Undo INSERT: remove the row, but only while the slot holds EXACTLY this
    // insert's bytes (length included). A mismatch means the insert was
    // already undone (re-run after a crash) or the slot was reused by a
    // committed transaction; prefix matching here would let a longer
    // committed row that merely starts with the loser's bytes be destroyed.
    // Exact matching is sufficient because both insert paths store the exact
    // length, and undo-UPDATE below re-normalizes the slot length when it
    // restores a before-image.
    if (slot_holds_exactly(table_page, rid.slot_id, record.new_tuple_data())) {
      applied = table_page.delete_record(rid.slot_id);
    }
    break;

  case LogRecordType::DELETE: {
    // Undo DELETE: restore the removed row, but only into a still-empty slot.
    // An occupied slot means the row was already restored or the slot was
    // reused by a committed transaction.
    uint16_t size = 0;
    if (table_page.get_record(rid.slot_id).empty() &&
        to_record_size(record.old_tuple_data().size(), &size)) {
      applied = table_page.insert_record_at(rid.slot_id,
                                            record.old_tuple_data().data(), size);
    }
    break;
  }

  case LogRecordType::UPDATE: {
    // Undo UPDATE: restore the before-image, but only while the slot holds
    // the after-image. This check stays prefix-based: an in-place shrink
    // leaves the slot's stale (longer) length over the after-image. A slot
    // already holding the before-image (re-run after a crash) or a committed
    // successor is left untouched.
    uint16_t size = 0;
    if (slot_holds(table_page, rid.slot_id, record.new_tuple_data()) &&
        to_record_size(record.old_tuple_data().size(), &size)) {
      // Restore via delete + insert-at rather than update_record: that stamps
      // the before-image's EXACT length back onto the slot (update_record
      // keeps a stale longer length on shrink), so the transaction's earlier
      // INSERT undo can rely on exact matching. Space always suffices: the
      // slot's stored record is at least as long as the before-image it once
      // held, and insert_record_at compacts to reclaim the freed hole.
      applied = table_page.delete_record(rid.slot_id) &&
                table_page.insert_record_at(rid.slot_id,
                                            record.old_tuple_data().data(), size);
      if (!applied) {
        buffer_pool_->unpin_page(rid.page_id, true);
        return Status::Internal("Undo UPDATE failed to restore before-image");
      }
    }
    break;
  }

  default:
    break;
  }

  if (applied) {
    ++undo_count_;
  } else {
    LOG_DEBUG("Undo skipped {} at ({},{}): slot no longer reflects the "
              "operation (already undone or reused)",
              log_record_type_to_string(record.type()), rid.page_id,
              rid.slot_id);
  }

  buffer_pool_->unpin_page(rid.page_id, applied);
  return Status::Ok();
}

} // namespace entropy
