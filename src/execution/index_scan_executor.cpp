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
IndexScanExecutor::IndexScanExecutor(ExecutorContext *ctx, BPlusTree *index,
                                     TableHeap *table_heap,
                                     const Schema *schema, BPTreeKey key)
    : Executor(ctx), index_(index), table_heap_(table_heap), schema_(schema),
      scan_type_(IndexScanType::POINT_LOOKUP), start_key_(key), end_key_(key) {}

// Range scan constructor
IndexScanExecutor::IndexScanExecutor(ExecutorContext *ctx, BPlusTree *index,
                                     TableHeap *table_heap,
                                     const Schema *schema, BPTreeKey start_key,
                                     BPTreeKey end_key)
    : Executor(ctx), index_(index), table_heap_(table_heap), schema_(schema),
      scan_type_(IndexScanType::RANGE_SCAN), start_key_(start_key),
      end_key_(end_key) {}

// Full scan constructor
IndexScanExecutor::IndexScanExecutor(ExecutorContext *ctx, BPlusTree *index,
                                     TableHeap *table_heap,
                                     const Schema *schema)
    : Executor(ctx), index_(index), table_heap_(table_heap), schema_(schema),
      scan_type_(IndexScanType::FULL_SCAN) {}

// ─────────────────────────────────────────────────────────────────────────────
// init() - Initialize scan position
// ─────────────────────────────────────────────────────────────────────────────

void IndexScanExecutor::init() {
  point_lookup_done_ = false;
  point_lookup_rid_ = std::nullopt;

  switch (scan_type_) {
  case IndexScanType::POINT_LOOKUP: {
    // O(log n) point lookup
    auto rid = index_->find(start_key_);
    if (rid.has_value()) {
      point_lookup_rid_ = rid;
    }
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
    // Point lookup returns at most one tuple
    if (point_lookup_done_ || !point_lookup_rid_.has_value()) {
      return std::nullopt;
    }
    point_lookup_done_ = true;
    return fetch_visible(*point_lookup_rid_);
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
