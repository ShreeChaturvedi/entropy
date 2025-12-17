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

#include "execution/executor.hpp"
#include "storage/b_plus_tree.hpp"
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
 * Uses B+ tree index for efficient lookups:
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
   * @param index B+ tree index
   * @param table_heap Table containing actual rows
   * @param schema Output schema
   * @param key Key value to look up
   */
  IndexScanExecutor(ExecutorContext *ctx, BPlusTree *index,
                    TableHeap *table_heap, const Schema *schema, BPTreeKey key);

  /**
   * @brief Construct for range scan
   * @param ctx Execution context
   * @param index B+ tree index
   * @param table_heap Table containing actual rows
   * @param schema Output schema
   * @param start_key Start of range (inclusive)
   * @param end_key End of range (inclusive)
   */
  IndexScanExecutor(ExecutorContext *ctx, BPlusTree *index,
                    TableHeap *table_heap, const Schema *schema,
                    BPTreeKey start_key, BPTreeKey end_key);

  /**
   * @brief Construct for full index scan
   * @param ctx Execution context
   * @param index B+ tree index
   * @param table_heap Table containing actual rows
   * @param schema Output schema
   */
  IndexScanExecutor(ExecutorContext *ctx, BPlusTree *index,
                    TableHeap *table_heap, const Schema *schema);

  void init() override;
  std::optional<Tuple> next() override;

  [[nodiscard]] const Schema *output_schema() const noexcept { return schema_; }

private:
  BPlusTree *index_;
  TableHeap *table_heap_;
  const Schema *schema_;

  IndexScanType scan_type_;
  BPTreeKey start_key_ = 0;
  BPTreeKey end_key_ = 0;

  // Iterator for range/full scans
  BPlusTreeIterator iterator_;
  BPlusTreeIterator end_iterator_;

  // For point lookup (may have duplicates if non-unique index)
  std::optional<RID> point_lookup_rid_;
  bool point_lookup_done_ = false;
};

} // namespace entropy
