/**
 * @file index_scan_executor.cpp
 * @brief High-performance Index Scan implementation
 *
 * Performance Characteristics:
 * - Point lookup: O(log n) B+ tree traversal + O(1) tuple fetch
 * - Range scan: O(log n) seek + O(k) iteration for k results
 * - Full scan: O(n) iteration through all leaf nodes
 *
 * The B+ tree provides RIDs, which are then used to fetch
 * actual tuples from the TableHeap with O(1) via buffer pool.
 */

#include "execution/index_scan_executor.hpp"

#include "execution/executor_context.hpp"

namespace entropy {

// ─────────────────────────────────────────────────────────────────────────────
// Constructors
// ─────────────────────────────────────────────────────────────────────────────

// Point lookup constructor
IndexScanExecutor::IndexScanExecutor(ExecutorContext *ctx, Index *index,
                                     TableHeap *table_heap,
                                     const Schema *schema, BPTreeKey key)
    : Executor(ctx), index_(index), table_heap_(table_heap), schema_(schema),
      scan_type_(IndexScanType::POINT_LOOKUP), start_key_(key), end_key_(key) {}

// Range scan constructor
IndexScanExecutor::IndexScanExecutor(ExecutorContext *ctx, Index *index,
                                     TableHeap *table_heap,
                                     const Schema *schema, BPTreeKey start_key,
                                     BPTreeKey end_key)
    : Executor(ctx), index_(index), table_heap_(table_heap), schema_(schema),
      scan_type_(IndexScanType::RANGE_SCAN), start_key_(start_key),
      end_key_(end_key) {}

// Full scan constructor
IndexScanExecutor::IndexScanExecutor(ExecutorContext *ctx, Index *index,
                                     TableHeap *table_heap,
                                     const Schema *schema)
    : Executor(ctx), index_(index), table_heap_(table_heap), schema_(schema),
      scan_type_(IndexScanType::FULL_SCAN) {}

// ─────────────────────────────────────────────────────────────────────────────
// init() - Initialize scan position
// ─────────────────────────────────────────────────────────────────────────────

void IndexScanExecutor::init() {
  point_lookup_rids_.clear();
  point_lookup_idx_ = 0;

  switch (scan_type_) {
  case IndexScanType::POINT_LOOKUP: {
    // All RIDs for the key (unique: 0 or 1; non-unique: every match).
    point_lookup_rids_ = index_->find_all(start_key_);
    break;
  }

  case IndexScanType::RANGE_SCAN: {
    // O(log n) seek to start of range
    iterator_ = index_->lower_bound(start_key_);
    end_iterator_ = index_->end();
    break;
  }

  case IndexScanType::FULL_SCAN: {
    // Start at beginning of index
    iterator_ = index_->begin();
    end_iterator_ = index_->end();
    break;
  }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// next() - Return next matching tuple
// ─────────────────────────────────────────────────────────────────────────────

std::optional<Tuple> IndexScanExecutor::fetch_visible(RID rid) {
  Tuple tuple;
  Status status = table_heap_->get_tuple(rid, &tuple);
  if (!status.ok()) {
    // Ghost probe, matching the seq scan's policy: only a transactional scan
    // asks the version store whether a freed slot still has a before-image
    // visible to its snapshot. Outside a transaction a missing tuple is
    // simply gone.
    if (ctx_ == nullptr || ctx_->txn == nullptr) {
      return std::nullopt;
    }
    return mvcc_visible(ctx_, Tuple({}, rid));
  }
  // The index still points at a RID whose heap version may be invisible to
  // this snapshot; the version store makes the call.
  return mvcc_visible(ctx_, tuple);
}

std::optional<Tuple> IndexScanExecutor::next() {
  switch (scan_type_) {
  case IndexScanType::POINT_LOOKUP: {
    while (point_lookup_idx_ < point_lookup_rids_.size()) {
      RID rid = point_lookup_rids_[point_lookup_idx_++];
      if (std::optional<Tuple> visible = fetch_visible(rid); visible) {
        return visible;
      }
    }
    return std::nullopt;
  }

  case IndexScanType::RANGE_SCAN: {
    // Iterate until end of range or end of index
    while (iterator_ != end_iterator_) {
      auto [key, rid] = *iterator_;
      ++iterator_;

      // Stop if past end of range
      if (key > end_key_) {
        return std::nullopt;
      }

      if (std::optional<Tuple> visible = fetch_visible(rid); visible) {
        return visible;
      }
      // Deleted or invisible to this snapshot; continue.
    }
    return std::nullopt;
  }

  case IndexScanType::FULL_SCAN: {
    // Iterate through all entries
    while (iterator_ != end_iterator_) {
      auto [key, rid] = *iterator_;
      ++iterator_;

      if (std::optional<Tuple> visible = fetch_visible(rid); visible) {
        return visible;
      }
      // Deleted or invisible to this snapshot; continue.
    }
    return std::nullopt;
  }
  }

  return std::nullopt;
}

} // namespace entropy
