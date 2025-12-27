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
   * @param schema Table schema
   * @param tuples Tuples to insert
   */
  InsertExecutor(ExecutorContext *ctx, std::shared_ptr<TableHeap> table_heap,
                 std::vector<Tuple> tuples)
      : Executor(ctx), table_heap_(std::move(table_heap)),
        tuples_(std::move(tuples)) {}

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

private:
  std::shared_ptr<TableHeap> table_heap_;
  std::vector<Tuple> tuples_;
  size_t current_idx_ = 0;
  size_t rows_inserted_ = 0;
  bool done_ = false;
};

} // namespace entropy
