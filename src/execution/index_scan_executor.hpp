#pragma once

/**
 * @file index_scan_executor.hpp
 * @brief High-performance Index Scan Executor
 *
 * Design Goals (Production RDBMS Quality):
 * 1. O(log n) seek time using B+ tree
 * 2. O(k) scan time where k = matching rows
 * 3. Support for point lookups (equality) and range scans
 * 4. Predicate pushdown for additional filtering
 * 5. Efficient RID-to-tuple lookup via TableHeap
 *
 * Scan Types:
 * - Point lookup: key = value
 * - Range scan: start_key <= key <= end_key
 * - Full index scan: iterate all entries
 *
 * Performance:
 * - Index seek: O(log n) tree traversal
 * - Range iteration: O(k) using leaf sibling pointers
 * - Tuple fetch: O(1) per RID via buffer pool
 */

#include <optional>
#include <vector>

#include "execution/executor.hpp"
#include "storage/index.hpp"
#include "storage/table_heap.hpp"

namespace entropy {

/**
 * @brief Scan type for index operations
 */
enum class IndexScanType {
  POINT_LOOKUP, // key = value (single row)
  RANGE_SCAN,   // start <= key <= end
  FULL_SCAN,    // iterate all entries
};

/**
 * @brief Index Scan Executor
 *
 * Uses an Index for efficient lookups:
 * - Point lookup: O(log n) to find single key
 * - Range scan: O(log n + k) for k results
 *
 * Each index entry is an RID pointing to table row.
 * Fetches actual tuples from TableHeap.
 */
class IndexScanExecutor : public Executor {
public:
  /**
   * @brief Construct for point lookup
   * @param ctx Execution context
   * @param index Secondary index
   * @param table_heap Table containing actual rows
   * @param schema Output schema
   * @param key Key value to look up
   */
  IndexScanExecutor(ExecutorContext *ctx, Index *index, TableHeap *table_heap,
                    const Schema *schema, BPTreeKey key);

  /**
   * @brief Construct for range scan
   * @param ctx Execution context
   * @param index Secondary index
   * @param table_heap Table containing actual rows
   * @param schema Output schema
   * @param start_key Start of range (inclusive)
   * @param end_key End of range (inclusive)
   */
  IndexScanExecutor(ExecutorContext *ctx, Index *index, TableHeap *table_heap,
                    const Schema *schema, BPTreeKey start_key,
                    BPTreeKey end_key);

  /**
   * @brief Construct for full index scan
   * @param ctx Execution context
   * @param index Secondary index
   * @param table_heap Table containing actual rows
   * @param schema Output schema
   */
  IndexScanExecutor(ExecutorContext *ctx, Index *index, TableHeap *table_heap,
                    const Schema *schema);

  void init() override;
  std::optional<Tuple> next() override;

  [[nodiscard]] const Schema *output_schema() const noexcept { return schema_; }

private:
  // Fetch the row at `rid` and resolve it against this snapshot. A missing
  // slot is probed as a ghost: its retained before-image may still be visible.
  [[nodiscard]] std::optional<Tuple> fetch_visible(RID rid);

  Index *index_;
  TableHeap *table_heap_;
  const Schema *schema_;

  IndexScanType scan_type_;
  BPTreeKey start_key_ = 0;
  BPTreeKey end_key_ = 0;

  // Iterator for range/full scans (type-erased over the concrete index)
  IndexIterator iterator_;
  IndexIterator end_iterator_;

  // Point lookup: all matching RIDs (size 0 or 1 for unique indexes)
  std::vector<RID> point_lookup_rids_;
  size_t point_lookup_idx_ = 0;
};

} // namespace entropy
