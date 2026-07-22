#pragma once

/**
 * @file insert_executor.hpp
 * @brief INSERT statement executor
 */

#include <vector>

#include "execution/executor.hpp"
#include "storage/table_heap.hpp"

namespace entropy {

/**
 * @brief Insert executor - inserts tuples into a table
 */
class InsertExecutor : public Executor {
public:
  /**
   * @brief Construct an INSERT executor
   * @param ctx Executor context
   * @param table_heap Table to insert into
   * @param tuples Tuples to insert
   * @param table_oid Table OID, used for row locking / WAL logging when a
   *        transaction context is present (INVALID_OID for context-less use)
   */
  InsertExecutor(ExecutorContext *ctx, std::shared_ptr<TableHeap> table_heap,
                 std::vector<Tuple> tuples, oid_t table_oid = INVALID_OID)
      : Executor(ctx), table_heap_(std::move(table_heap)),
        tuples_(std::move(tuples)), table_oid_(table_oid) {}

  /**
   * @brief Initialize the executor
   */
  void init() override;

  /**
   * @brief Execute insertions and return count
   * @return Tuple containing insert count, or nullopt when done
   */
  [[nodiscard]] std::optional<Tuple> next() override;

  /**
   * @brief Get number of rows inserted
   */
  [[nodiscard]] size_t rows_inserted() const { return rows_inserted_; }

  /**
   * @brief First error encountered (lock failure or write-write conflict)
   *
   * Ok() unless a transactional concern aborted the insert; the API layer turns
   * a non-ok status into a transaction abort.
   */
  [[nodiscard]] Status status() const { return status_; }

private:
  std::shared_ptr<TableHeap> table_heap_;
  std::vector<Tuple> tuples_;
  oid_t table_oid_ = INVALID_OID;
  size_t current_idx_ = 0;
  size_t rows_inserted_ = 0;
  bool done_ = false;
  Status status_ = Status::Ok();
};

} // namespace entropy
