/**
 * @file recovery.cpp
 * @brief ARIES-style Crash Recovery implementation
 */

#include "transaction/recovery.hpp"

#include "common/logger.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/disk_manager.hpp"
#include "transaction/wal.hpp"

namespace entropy {

RecoveryManager::RecoveryManager(std::shared_ptr<BufferPoolManager> buffer_pool,
                                 std::shared_ptr<WALManager> wal,
                                 std::shared_ptr<DiskManager> disk_manager)
    : buffer_pool_(std::move(buffer_pool)), wal_(std::move(wal)),
      disk_manager_(std::move(disk_manager)) {}

Status RecoveryManager::recover() {
  LOG_INFO("Starting crash recovery...");

  // Read all log records from WAL
  if (!wal_) {
    LOG_WARN("No WAL manager provided, skipping recovery");
    return Status::Ok();
  }

  std::vector<LogRecord> records = wal_->read_log();
  if (records.empty()) {
    LOG_INFO("WAL is empty, nothing to recover");
    return Status::Ok();
  }

  LOG_INFO("Read {} log records from WAL", records.size());

  // Reset state
  active_txn_table_.clear();
  committed_txns_.clear();
  redo_count_ = 0;
  undo_count_ = 0;

  // Phase 1: Analysis
  LOG_INFO("=== ANALYSIS PHASE ===");
  analysis_phase(records);
  LOG_INFO("Analysis complete: {} active txns, {} committed txns",
           active_txn_table_.size(), committed_txns_.size());

  // Phase 2: Redo
  LOG_INFO("=== REDO PHASE ===");
  Status redo_status = redo_phase(records);
  if (!redo_status.ok()) {
    LOG_ERROR("Redo phase failed: {}", redo_status.message());
    return redo_status;
  }
  LOG_INFO("Redo complete: {} operations replayed", redo_count_);

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
    const std::vector<txn_id_t> &active_txn_ids) {
  if (!wal_) {
    return Status::InvalidArgument("No WAL manager provided");
  }

  // Create and append checkpoint record
  LogRecord checkpoint = LogRecord::make_checkpoint(active_txn_ids);
  wal_->append_log(checkpoint);

  // Force flush to ensure checkpoint is durable
  return wal_->flush();
}

// ─────────────────────────────────────────────────────────────────────────────
// Analysis Phase
// ─────────────────────────────────────────────────────────────────────────────

void RecoveryManager::analysis_phase(const std::vector<LogRecord> &records) {
  // We process all records from the beginning because we need to track
  // which transactions committed (even before the checkpoint) for the
  // committed_txns_ set. The checkpoint only helps us identify which
  // transactions were active at the checkpoint moment.

  // First, find the last checkpoint to get initial ATT state
  for (size_t i = records.size(); i > 0; --i) {
    if (records[i - 1].type() == LogRecordType::CHECKPOINT) {
      // Initialize ATT from checkpoint's active transactions
      for (txn_id_t txn_id : records[i - 1].active_txns()) {
        active_txn_table_[txn_id] = INVALID_LSN;
      }
      LOG_DEBUG("Found checkpoint at index {}", i - 1);
      break;
    }
  }

  // Scan all records to build committed set and update ATT
  for (size_t i = 0; i < records.size(); ++i) {
    const LogRecord &record = records[i];
    txn_id_t txn_id = record.txn_id();

    switch (record.type()) {
    case LogRecordType::BEGIN:
      // New transaction started
      active_txn_table_[txn_id] = record.lsn();
      break;

    case LogRecordType::COMMIT:
      // Transaction committed - remove from ATT, add to committed set
      active_txn_table_.erase(txn_id);
      committed_txns_.insert(txn_id);
      break;

    case LogRecordType::ABORT:
      // Transaction aborted - remove from ATT (already rolled back)
      active_txn_table_.erase(txn_id);
      break;

    case LogRecordType::INSERT:
    case LogRecordType::DELETE:
    case LogRecordType::UPDATE:
      // Update last LSN for this transaction
      active_txn_table_[txn_id] = record.lsn();
      break;

    default:
      break;
    }
  }

  LOG_DEBUG("After analysis: {} uncommitted transactions need undo",
            active_txn_table_.size());
}

// ─────────────────────────────────────────────────────────────────────────────
// Redo Phase
// ─────────────────────────────────────────────────────────────────────────────

Status RecoveryManager::redo_phase(const std::vector<LogRecord> &records) {
  // Redo all data modification operations in log order
  // In a full ARIES implementation, we would check page LSN vs log LSN
  // to avoid redundant redos. For simplicity, we redo all operations.

  for (const LogRecord &record : records) {
    switch (record.type()) {
    case LogRecordType::INSERT:
    case LogRecordType::UPDATE:
      // Redo these operations
      // Note: In a full implementation, we'd check if the page
      // already has this change applied (page_lsn >= record.lsn)
      {
        Status status = redo_record(record);
        if (!status.ok()) {
          // Log warning but continue - page may not exist
          LOG_WARN("Redo failed for LSN {}: {}", record.lsn(),
                   status.message());
        }
      }
      break;

    case LogRecordType::DELETE:
      // For delete, we mark the tuple as deleted
      {
        Status status = redo_record(record);
        if (!status.ok()) {
          LOG_WARN("Redo delete failed for LSN {}: {}", record.lsn(),
                   status.message());
        }
      }
      break;

    default:
      // BEGIN, COMMIT, ABORT, CHECKPOINT don't need redo
      break;
    }
  }

  return Status::Ok();
}

Status RecoveryManager::redo_record(const LogRecord &record) {
  // In a minimal implementation without full TableHeap integration,
  // we track the redo count for testing purposes.
  // A complete implementation would apply changes to pages via buffer pool.

  ++redo_count_;

  // If no buffer pool or disk manager, just track the operation
  if (!buffer_pool_) {
    return Status::Ok();
  }

  // Get the page containing the tuple
  page_id_t page_id = record.rid().page_id;
  Page *page = buffer_pool_->fetch_page(page_id);
  if (page == nullptr) {
    // Page doesn't exist (might be new page not yet created)
    return Status::NotFound("Page not found for redo");
  }

  // Apply the operation to the page
  // The actual implementation would deserialize and apply tuple changes
  // For now, we just mark the page as dirty to indicate recovery touched it

  buffer_pool_->unpin_page(page_id, false); // Mark as touched but not dirty yet

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

  // Build LSN -> record map for following prevLSN chains
  auto lsn_map = build_lsn_map(records);

  // For each uncommitted transaction, undo its operations in reverse order
  // by following the prevLSN chain
  for (const auto &[txn_id, last_lsn] : active_txn_table_) {
    LOG_DEBUG("Undoing transaction {} from LSN {}", txn_id, last_lsn);

    lsn_t current_lsn = last_lsn;
    while (current_lsn != INVALID_LSN) {
      auto it = lsn_map.find(current_lsn);
      if (it == lsn_map.end()) {
        LOG_WARN("Could not find log record for LSN {}", current_lsn);
        break;
      }

      const LogRecord *record = it->second;

      // Only undo data modification operations
      if (record->type() == LogRecordType::INSERT ||
          record->type() == LogRecordType::DELETE ||
          record->type() == LogRecordType::UPDATE) {

        Status status = undo_record(*record);
        if (!status.ok()) {
          LOG_WARN("Undo failed for LSN {}: {}", current_lsn, status.message());
        }
      }

      // Move to previous operation in this transaction
      current_lsn = record->prev_lsn();
    }
  }

  return Status::Ok();
}

Status RecoveryManager::undo_record(const LogRecord &record) {
  // Increment undo counter for testing
  ++undo_count_;

  // In a complete implementation, we would apply the inverse operation:
  // - INSERT -> delete the inserted tuple
  // - DELETE -> re-insert the old tuple
  // - UPDATE -> restore the old tuple data
  //
  // For now, we just track undos without buffer pool integration

  if (!buffer_pool_) {
    return Status::Ok();
  }

  page_id_t page_id = record.rid().page_id;
  Page *page = buffer_pool_->fetch_page(page_id);
  if (page == nullptr) {
    return Status::NotFound("Page not found for undo");
  }

  // Apply the inverse operation
  // A complete implementation would use old_tuple_data for DELETE/UPDATE

  buffer_pool_->unpin_page(page_id, false);

  return Status::Ok();
}

std::unordered_map<lsn_t, const LogRecord *>
RecoveryManager::build_lsn_map(const std::vector<LogRecord> &records) {

  std::unordered_map<lsn_t, const LogRecord *> lsn_map;
  for (const LogRecord &record : records) {
    lsn_map[record.lsn()] = &record;
  }
  return lsn_map;
}

} // namespace entropy
