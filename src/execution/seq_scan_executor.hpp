#pragma once

/**
 * @file seq_scan_executor.hpp
 * @brief Sequential scan executor - scans all tuples in a table
 */

#include "execution/executor.hpp"
#include "parser/expression.hpp"
#include "storage/table_heap.hpp"

namespace entropy {

/**
 * @brief Sequential scan executor
 *
 * Iterates through all tuples in a TableHeap, optionally
 * filtering with a predicate expression.
 */
class SeqScanExecutor : public Executor {
public:
  /**
   * @brief Construct a SeqScan executor
   * @param ctx Executor context
   * @param table_heap Table to scan
   * @param schema Table schema
   * @param predicate Optional filter predicate (WHERE clause)
   */
  SeqScanExecutor(ExecutorContext *ctx, std::shared_ptr<TableHeap> table_heap,
                  const Schema *schema,
                  std::unique_ptr<Expression> predicate = nullptr)
      : Executor(ctx), table_heap_(std::move(table_heap)), schema_(schema),
        predicate_(std::move(predicate)) {}

  /**
   * @brief Initialize the scan
   */
  void init() override;

  /**
   * @brief Get the next tuple that satisfies the predicate
   * @return Next tuple, or nullopt if no more tuples
   */
  [[nodiscard]] std::optional<Tuple> next() override;

  /**
   * @brief Get the output schema
   */
  [[nodiscard]] const Schema *output_schema() const { return schema_; }

private:
  std::shared_ptr<TableHeap> table_heap_;
  const Schema *schema_;
  std::unique_ptr<Expression> predicate_;
  TableIterator iterator_;
  TableIterator end_;
};

} // namespace entropy
