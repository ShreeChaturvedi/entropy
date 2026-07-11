#pragma once

/**
 * @file delete_executor.hpp
 * @brief DELETE statement executor
 */

#include <memory>

#include "execution/executor.hpp"
#include "parser/expression.hpp"
#include "storage/table_heap.hpp"

namespace entropy {

/**
 * @brief Delete executor - deletes tuples matching a predicate
 *
 * Uses a child executor (typically SeqScan) to find tuples to delete.
 */
class DeleteExecutor : public Executor {
public:
  /**
   * @brief Construct a DELETE executor
   * @param ctx Executor context
   * @param child Child executor providing tuples to delete
   * @param table_heap Table to delete from
   */
  DeleteExecutor(ExecutorContext *ctx, std::unique_ptr<Executor> child,
                 std::shared_ptr<TableHeap> table_heap,
                 oid_t table_oid = INVALID_OID)
      : Executor(ctx), child_(std::move(child)),
        table_heap_(std::move(table_heap)), table_oid_(table_oid) {}

  /**
   * @brief Initialize the executor
   */
  void init() override;

  /**
   * @brief Execute deletions
   * @return nullopt when done
   */
  [[nodiscard]] std::optional<Tuple> next() override;

  /**
   * @brief Get number of rows deleted
   */
  [[nodiscard]] size_t rows_deleted() const { return rows_deleted_; }

  /**
   * @brief First error encountered (lock failure or write-write conflict)
   */
  [[nodiscard]] Status status() const { return status_; }

private:
  std::unique_ptr<Executor> child_;
  std::shared_ptr<TableHeap> table_heap_;
  oid_t table_oid_ = INVALID_OID;
  size_t rows_deleted_ = 0;
  bool done_ = false;
  Status status_ = Status::Ok();
};

} // namespace entropy
