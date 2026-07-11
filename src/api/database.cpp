/**
 * @file database.cpp
 * @brief Database class implementation with SQL execution
 *
 * DatabaseImpl owns the full engine stack: storage (disk manager + buffer
 * pool), durability (WAL + recovery on open), concurrency (lock manager,
 * MVCC version store, transaction manager), and the query layers (catalog,
 * binder, optimizer, executors). Every DML/query statement runs inside a
 * transaction: an explicit one opened by begin_transaction() on the calling
 * thread, or an implicit autocommit transaction spanning just that statement.
 */

#include "entropy/database.hpp"

#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"
#include "execution/aggregation.hpp"
#include "execution/delete_executor.hpp"
#include "execution/executor_context.hpp"
#include "execution/filter.hpp"
#include "execution/hash_join.hpp"
#include "execution/index_scan_executor.hpp"
#include "execution/insert_executor.hpp"
#include "execution/limit_executor.hpp"
#include "execution/nested_loop_join.hpp"
#include "execution/projection.hpp"
#include "execution/seq_scan_executor.hpp"
#include "execution/sort_executor.hpp"
#include "execution/update_executor.hpp"
#include "optimizer/cost_model.hpp"
#include "optimizer/index_selector.hpp"
#include "optimizer/optimizer.hpp"
#include "optimizer/statistics.hpp"
#include "parser/binder.hpp"
#include "parser/parser.hpp"
#include "parser/statement.hpp"
#include "storage/buffer_pool.hpp"
#include "storage/disk_manager.hpp"
#include "storage/table_heap.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/mvcc.hpp"
#include "transaction/recovery.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction/version_store.hpp"
#include "transaction/wal.hpp"

namespace entropy {

namespace {

// Map a planner join-type back to the parser JoinType the executors accept.
JoinType to_parser_join_type(NestedLoopJoinPlanNode::JoinType t) {
  switch (t) {
  case NestedLoopJoinPlanNode::JoinType::INNER:
    return JoinType::INNER;
  case NestedLoopJoinPlanNode::JoinType::LEFT:
    return JoinType::LEFT;
  case NestedLoopJoinPlanNode::JoinType::RIGHT:
    return JoinType::RIGHT;
  case NestedLoopJoinPlanNode::JoinType::CROSS:
    return JoinType::CROSS;
  }
  return JoinType::INNER;
}

JoinType to_parser_join_type(HashJoinPlanNode::JoinType t) {
  switch (t) {
  case HashJoinPlanNode::JoinType::INNER:
    return JoinType::INNER;
  case HashJoinPlanNode::JoinType::LEFT:
    return JoinType::LEFT;
  case HashJoinPlanNode::JoinType::RIGHT:
    return JoinType::RIGHT;
  }
  return JoinType::INNER;
}

NestedLoopJoinPlanNode::JoinType to_plan_join_type(JoinType t) {
  switch (t) {
  case JoinType::INNER:
    return NestedLoopJoinPlanNode::JoinType::INNER;
  case JoinType::LEFT:
    return NestedLoopJoinPlanNode::JoinType::LEFT;
  case JoinType::RIGHT:
    return NestedLoopJoinPlanNode::JoinType::RIGHT;
  case JoinType::CROSS:
    return NestedLoopJoinPlanNode::JoinType::CROSS;
  }
  return NestedLoopJoinPlanNode::JoinType::INNER;
}

// Resolve every ColumnRefExpression in @p expr against @p schema, setting its
// column index and type. @p col_tables holds the source-relation alias of each
// schema column (parallel to the columns) so a qualified reference `t.col`
// binds to the column of relation @c t; an unqualified reference binds to the
// first column with a matching name.
Status resolve_expr(Expression *expr, const Schema &schema,
                    const std::vector<std::string> &col_tables) {
  if (expr == nullptr) {
    return Status::Ok();
  }
  switch (expr->expr_type()) {
  case ExpressionType::CONSTANT:
  case ExpressionType::UNARY:
    return Status::Ok();
  case ExpressionType::COLUMN_REF: {
    auto *col = dynamic_cast<ColumnRefExpression *>(expr);
    if (col == nullptr) {
      return Status::InvalidArgument("Invalid column reference expression");
    }
    const std::string &want_table = col->table_name();
    const std::string &want_col = col->column_name();
    int found = -1;
    for (size_t i = 0; i < schema.column_count(); ++i) {
      if (schema.column(i).name() != want_col) {
        continue;
      }
      if (!want_table.empty()) {
        if (i < col_tables.size() && col_tables[i] == want_table) {
          found = static_cast<int>(i);
          break;
        }
        continue;
      }
      found = static_cast<int>(i);
      break;
    }
    if (found < 0) {
      const std::string full =
          want_table.empty() ? want_col : want_table + "." + want_col;
      return Status::NotFound("Column not found: " + full);
    }
    col->set_column_index(static_cast<size_t>(found));
    col->set_type(schema.column(static_cast<size_t>(found)).type());
    return Status::Ok();
  }
  case ExpressionType::BINARY_OP: {
    auto *b = dynamic_cast<BinaryOpExpression *>(expr);
    if (b == nullptr) {
      return Status::InvalidArgument("Invalid binary expression");
    }
    Status s = resolve_expr(const_cast<Expression *>(b->left()), schema,
                            col_tables);
    if (!s.ok()) {
      return s;
    }
    return resolve_expr(const_cast<Expression *>(b->right()), schema,
                        col_tables);
  }
  case ExpressionType::COMPARISON: {
    auto *c = dynamic_cast<ComparisonExpression *>(expr);
    if (c == nullptr) {
      return Status::InvalidArgument("Invalid comparison expression");
    }
    Status s = resolve_expr(const_cast<Expression *>(c->left()), schema,
                            col_tables);
    if (!s.ok()) {
      return s;
    }
    return resolve_expr(const_cast<Expression *>(c->right()), schema,
                        col_tables);
  }
  case ExpressionType::LOGICAL: {
    auto *l = dynamic_cast<LogicalExpression *>(expr);
    if (l == nullptr) {
      return Status::InvalidArgument("Invalid logical expression");
    }
    Status s = resolve_expr(const_cast<Expression *>(l->left()), schema,
                            col_tables);
    if (!s.ok()) {
      return s;
    }
    return resolve_expr(const_cast<Expression *>(l->right()), schema,
                        col_tables);
  }
  case ExpressionType::IS_NULL: {
    auto *n = dynamic_cast<IsNullExpression *>(expr);
    if (n == nullptr) {
      return Status::InvalidArgument("Invalid IS NULL expression");
    }
    return resolve_expr(const_cast<Expression *>(n->operand()), schema,
                        col_tables);
  }
  }
  return Status::Ok();
}

// Translate a parsed aggregate function into the execution-layer enum.
AggregateType to_exec_aggregate(AggregateFunc fn) {
  switch (fn) {
  case AggregateFunc::COUNT_STAR:
    return AggregateType::COUNT_STAR;
  case AggregateFunc::COUNT:
    return AggregateType::COUNT;
  case AggregateFunc::SUM:
    return AggregateType::SUM;
  case AggregateFunc::AVG:
    return AggregateType::AVG;
  case AggregateFunc::MIN:
    return AggregateType::MIN;
  case AggregateFunc::MAX:
  case AggregateFunc::NONE:
    return AggregateType::MAX;
  }
  return AggregateType::MAX;
}

} // namespace

class DatabaseImpl {
public:
  // @param injected_disk When non-null, used as the storage backend instead of
  //   opening a FileDiskManager on @p path (dependency injection for tests and
  //   alternative backends; see DiskManager). The WAL/catalog file paths are
  //   still derived from @p path.
  explicit DatabaseImpl(const std::string &path, const DatabaseOptions &options,
                        std::shared_ptr<DiskManager> injected_disk = nullptr)
      : path_(path), strict_mode_(options.strict_mode), is_open_(false) {
    Logger::init();
    LOG_INFO("Opening database: {}", path);

    // The storage engine is compiled for a fixed page size (Page is a
    // std::array<char, kDefaultPageSize>); any other request must fail loudly
    // instead of silently running at the built-in size.
    if (options.page_size != config::kDefaultPageSize) {
      open_error_ = "Unsupported page_size " +
                    std::to_string(options.page_size) +
                    ": this build supports only " +
                    std::to_string(config::kDefaultPageSize) +
                    "-byte pages (compile-time constant)";
      LOG_ERROR("{}", open_error_);
      return;
    }

    // Storage layer. An injected backend wins; otherwise create_if_missing/
    // error_if_exists route straight into the file-open logic; on violation the
    // manager stays closed.
    if (injected_disk != nullptr) {
      disk_manager_ = std::move(injected_disk);
    } else {
      disk_manager_ = std::make_shared<FileDiskManager>(
          path, options.create_if_missing, options.error_if_exists);
    }
    if (!disk_manager_->is_open()) {
      open_error_ = "Failed to open database file: " + path;
      LOG_ERROR("{}", open_error_);
      return;
    }

    buffer_pool_ = std::make_shared<BufferPoolManager>(options.buffer_pool_size,
                                                       disk_manager_);

    // Concurrency control: one lock manager, one logical clock, one version
    // store shared by every transaction.
    lock_manager_ = std::make_unique<LockManager>();
    mvcc_ = std::make_shared<MVCCManager>();
    version_store_ = std::make_shared<VersionStore>(*mvcc_);

    // Durability: WAL (gated on enable_wal — when off, no .wal file is ever
    // created or written) and the WAL-before-page hook on the buffer pool.
    if (options.enable_wal) {
      wal_manager_ =
          std::make_shared<WALManager>(path + config::kWALFileExtension);
      // Steal rule: before any dirty page reaches disk, the log must be
      // durable up to that page's LSN. The hook captures the WAL by
      // shared_ptr so it outlives the buffer pool's final destructor flush.
      buffer_pool_->set_wal_flush_hook([wal = wal_manager_](lsn_t lsn) {
        if (lsn == INVALID_LSN) {
          return Status::Ok(); // page carries no logged change
        }
        return wal->flush_to_lsn(lsn);
      });
      txn_manager_ = std::make_unique<TransactionManager>(wal_manager_);
    } else {
      txn_manager_ = std::make_unique<TransactionManager>();
    }
    txn_manager_->set_lock_manager(lock_manager_.get());
    txn_manager_->set_mvcc(mvcc_);
    txn_manager_->set_version_store(version_store_);
    txn_manager_->set_buffer_pool(buffer_pool_.get());

    // Crash recovery, BEFORE the catalog is constructed: recovery is
    // catalog-independent (it replays raw pages by page id), and the
    // catalog's heap-chain walk must observe recovered pages, not stale ones.
    if (wal_manager_) {
      RecoveryManager recovery(buffer_pool_, wal_manager_, disk_manager_);
      Status recovered = recovery.recover();
      if (!recovered.ok()) {
        open_error_ =
            "Crash recovery failed: " + std::string(recovered.message());
        LOG_ERROR("{}", open_error_);
        return;
      }
      // Post-restart transactions must never alias recovered ids (#19).
      txn_manager_->seed_next_txn_id(recovery.next_txn_id());

      // Startup checkpoint: makes the recovered state durable (flushing the
      // pages recovery mutated, which also re-extends the on-disk file over
      // recovered page ids) and anchors the next recovery past the records
      // just replayed. Failure is non-fatal — the WAL still holds everything
      // — so log and continue.
      Status checkpointed = recovery.create_checkpoint({});
      if (!checkpointed.ok()) {
        LOG_WARN("Startup checkpoint failed (recovery unaffected): {}",
                 checkpointed.message());
      }
    }

    // Catalog (durable manifest alongside the database file) and binder.
    catalog_ = std::make_shared<Catalog>(buffer_pool_,
                                         path + config::kCatalogFileExtension);
    binder_ = std::make_unique<Binder>(catalog_.get(), strict_mode_);

    // Abort undo resolves table oids through the catalog. The shared_ptr
    // keeps the TableInfo (and its heap) alive across a concurrent drop.
    txn_manager_->set_table_resolver(
        [catalog = catalog_](oid_t table_oid) -> TableHeap * {
          auto info = catalog->get_table_shared(table_oid);
          return info ? info->table_heap.get() : nullptr;
        });

    // Initialize optimizer components
    statistics_ = std::make_shared<Statistics>(catalog_);
    cost_model_ = std::make_shared<CostModel>(statistics_);
    index_selector_ =
        std::make_unique<IndexSelector>(catalog_, statistics_, cost_model_);
    optimizer_ = std::make_unique<Optimizer>(catalog_, statistics_, cost_model_,
                                             index_selector_.get());

    is_open_ = true;
  }

  ~DatabaseImpl() { close(); }

  Result execute(std::string_view sql) {
    if (!is_open_) {
      return Result(Status::InvalidArgument(
          open_error_.empty() ? "Database is not open" : open_error_));
    }

    // Parse SQL
    Parser parser(sql);
    std::unique_ptr<Statement> stmt;
    Status status = parser.parse(&stmt);
    if (!status.ok()) {
      return Result(status);
    }

    // DDL and EXPLAIN run outside the transaction machinery: the catalog has
    // its own durability (manifest fsync per DDL), and EXPLAIN only binds.
    switch (stmt->type()) {
    case StatementType::CREATE_TABLE:
      return execute_create_table(
          dynamic_cast<CreateTableStatement *>(stmt.get()));
    case StatementType::DROP_TABLE:
      return execute_drop_table(dynamic_cast<DropTableStatement *>(stmt.get()));
    case StatementType::EXPLAIN:
      return execute_explain(dynamic_cast<ExplainStatement *>(stmt.get()));
    case StatementType::SELECT:
    case StatementType::INSERT:
    case StatementType::UPDATE:
    case StatementType::DELETE_STMT:
      break;
    default:
      return Result(Status::NotSupported("Unsupported statement type"));
    }

    // Every DML/query statement runs inside a transaction: the calling
    // thread's explicit one if open, otherwise an implicit autocommit
    // transaction spanning just this statement. A failed explicit
    // transaction (aborted by an earlier error) rejects statements until the
    // caller rolls it back — it must never fall through to autocommit.
    const SessionState session = session_state();
    if (session.bound && session.txn == nullptr) {
      return Result(Status::Aborted(
          "Current transaction is aborted; statements are rejected until "
          "rollback"));
    }
    Transaction *txn = session.txn;
    const bool autocommit = (txn == nullptr);
    if (autocommit) {
      txn = txn_manager_->begin();
      if (txn == nullptr) {
        return Result(Status::Internal("Failed to begin transaction"));
      }
    }

    ExecutorContext exec_ctx{txn, txn_manager_.get(), lock_manager_.get(),
                             version_store_.get(), catalog_.get()};

    Result result = [&]() -> Result {
      switch (stmt->type()) {
      case StatementType::SELECT:
        return execute_select(dynamic_cast<SelectStatement *>(stmt.get()),
                              &exec_ctx);
      case StatementType::INSERT:
        return execute_insert(dynamic_cast<InsertStatement *>(stmt.get()),
                              &exec_ctx);
      case StatementType::UPDATE:
        return execute_update(dynamic_cast<UpdateStatement *>(stmt.get()),
                              &exec_ctx);
      case StatementType::DELETE_STMT:
        return execute_delete(dynamic_cast<DeleteStatement *>(stmt.get()),
                              &exec_ctx);
      default:
        return Result(Status::Internal("Unreachable statement type"));
      }
    }();

    if (!result.ok()) {
      // Executor error or write-write conflict: the transaction's writes may
      // be partially applied, so abort it (write-set undo + version rollback
      // + lock release) and surface the error. An explicit transaction stays
      // BOUND in a failed state: subsequent statements are rejected and only
      // rollback (or commit, which errors) clears it, so the caller can
      // never silently fall back to autocommit mid-transaction.
      if (!autocommit) {
        mark_thread_txn_failed();
      }
      txn_manager_->abort(txn);
      return result;
    }

    if (autocommit) {
      txn_manager_->commit(txn);
    }
    return result;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Statement Execution
  // ─────────────────────────────────────────────────────────────────────────

  // Apply ORDER BY (resolved by name against @p schema) then LIMIT/OFFSET, and
  // materialize the result. Shared by the join/aggregation pipelines, which
  // both order and limit their already-projected output.
  Result finalize_query(std::unique_ptr<Executor> executor,
                        const Schema *schema, SelectStatement *stmt,
                        ExecutorContext *ctx) {
    if (!stmt->order_by.empty()) {
      std::vector<SortKey> sort_keys;
      sort_keys.reserve(stmt->order_by.size());
      for (const auto &item : stmt->order_by) {
        int idx = schema->get_column_index(item.column_name);
        if (idx < 0) {
          return Result(Status::InvalidArgument("ORDER BY column not found: " +
                                                item.column_name));
        }
        sort_keys.emplace_back(static_cast<size_t>(idx), item.ascending);
      }
      executor = std::make_unique<SortExecutor>(ctx, std::move(executor), schema,
                                                std::move(sort_keys));
    }
    if (stmt->limit.has_value() || stmt->offset.has_value()) {
      executor = std::make_unique<LimitExecutor>(
          ctx, std::move(executor), stmt->limit, stmt->offset.value_or(0));
    }
    return collect_result(executor.get(), schema);
  }

  // Drain @p executor and materialize the rows under @p output_schema.
  Result collect_result(Executor *executor, const Schema *output_schema) {
    executor->init();
    std::vector<std::string> column_names;
    column_names.reserve(output_schema->column_count());
    for (size_t i = 0; i < output_schema->column_count(); i++) {
      column_names.push_back(output_schema->column(i).name());
    }

    std::vector<Row> rows;
    while (auto tuple = executor->next()) {
      std::vector<Value> values;
      values.reserve(output_schema->column_count());
      for (size_t i = 0; i < output_schema->column_count(); i++) {
        TupleValue tv =
            tuple->get_value(*output_schema, static_cast<uint32_t>(i));
        values.push_back(tuple_value_to_value(tv));
      }
      rows.emplace_back(std::move(values), column_names);
    }
    return Result(std::move(rows), std::move(column_names));
  }

  // Turn a scan/join physical plan node into an executor tree. Only scan and
  // join nodes appear here; aggregation/projection/sort/limit are layered on
  // top by the SELECT paths.
  std::unique_ptr<Executor> build_plan_executor(const PlanNode *node,
                                                ExecutorContext *ctx) {
    switch (node->type()) {
    case PlanNodeType::SEQ_SCAN: {
      const auto *s = static_cast<const SeqScanPlanNode *>(node);
      auto *info = catalog_->get_table(s->table_oid());
      if (info == nullptr) {
        return nullptr;
      }
      std::unique_ptr<Expression> pred =
          s->predicate() ? s->predicate()->clone() : nullptr;
      return std::make_unique<SeqScanExecutor>(
          ctx, info->table_heap, s->output_schema(), std::move(pred));
    }
    case PlanNodeType::INDEX_SCAN: {
      const auto *s = static_cast<const IndexScanPlanNode *>(node);
      auto *info = catalog_->get_table(s->table_oid());
      auto *index_info = catalog_->get_index_by_oid(s->index_oid());
      if (info == nullptr || index_info == nullptr || !index_info->index) {
        return nullptr;
      }
      switch (s->scan_type()) {
      case IndexScanPlanNode::ScanType::POINT_LOOKUP:
        return std::make_unique<IndexScanExecutor>(
            ctx, index_info->index.get(), info->table_heap.get(),
            s->output_schema(),
            s->start_key().value_or(std::numeric_limits<BPTreeKey>::min()));
      case IndexScanPlanNode::ScanType::RANGE_SCAN:
        return std::make_unique<IndexScanExecutor>(
            ctx, index_info->index.get(), info->table_heap.get(),
            s->output_schema(),
            s->start_key().value_or(std::numeric_limits<BPTreeKey>::min()),
            s->end_key().value_or(std::numeric_limits<BPTreeKey>::max()));
      case IndexScanPlanNode::ScanType::FULL_SCAN:
        return std::make_unique<IndexScanExecutor>(
            ctx, index_info->index.get(), info->table_heap.get(),
            s->output_schema());
      }
      return nullptr;
    }
    case PlanNodeType::NESTED_LOOP_JOIN: {
      const auto *j = static_cast<const NestedLoopJoinPlanNode *>(node);
      auto left = build_plan_executor(node->children()[0].get(), ctx);
      auto right = build_plan_executor(node->children()[1].get(), ctx);
      std::unique_ptr<Expression> cond =
          j->condition() ? j->condition()->clone() : nullptr;
      return std::make_unique<NestedLoopJoinExecutor>(
          ctx, std::move(left), std::move(right),
          node->children()[0]->output_schema(),
          node->children()[1]->output_schema(), j->output_schema(),
          to_parser_join_type(j->join_type()), std::move(cond));
    }
    case PlanNodeType::HASH_JOIN: {
      const auto *j = static_cast<const HashJoinPlanNode *>(node);
      auto left = build_plan_executor(node->children()[0].get(), ctx);
      auto right = build_plan_executor(node->children()[1].get(), ctx);
      return std::make_unique<HashJoinExecutor>(
          ctx, std::move(left), std::move(right),
          node->children()[0]->output_schema(),
          node->children()[1]->output_schema(), j->output_schema(),
          j->left_key_index(), j->right_key_index(),
          to_parser_join_type(j->join_type()));
    }
    default:
      return nullptr;
    }
  }

  // Plan a single-table scan through the optimizer and instantiate it.
  std::unique_ptr<Executor> build_scan_executor(oid_t table_oid,
                                                const Schema *schema,
                                                std::unique_ptr<Expression> filter,
                                                ExecutorContext *ctx,
                                                Status *status) {
    std::vector<ScanSpec> relations;
    relations.push_back(ScanSpec{table_oid, schema, std::move(filter)});
    std::unique_ptr<PlanNode> plan;
    *status = optimizer_->optimize(std::move(relations), {}, &plan);
    if (!status->ok()) {
      return nullptr;
    }
    return build_plan_executor(plan.get(), ctx);
  }

  Result execute_select(SelectStatement *stmt, ExecutorContext *exec_ctx) {
    // Route queries that need joins, grouping, aggregation, or computed/aliased
    // projections through the general pipeline; everything else takes the
    // single-table fast path.
    bool has_aggregate = false;
    bool has_computed = false;
    for (const auto &col : stmt->columns) {
      if (col.agg_func != AggregateFunc::NONE) {
        has_aggregate = true;
      }
      if (col.expression != nullptr || !col.alias.empty()) {
        has_computed = true;
      }
    }
    if (!stmt->joins.empty() || !stmt->group_by.empty() || has_aggregate ||
        has_computed) {
      return execute_select_query(stmt, exec_ctx);
    }
    return execute_select_simple(stmt, exec_ctx);
  }

  // Single-table SELECT with plain column projection (no joins/aggregation/
  // computed expressions). The scan access path is chosen by the optimizer.
  Result execute_select_simple(SelectStatement *stmt,
                               ExecutorContext *exec_ctx) {
    BoundSelectContext ctx;
    Status status = binder_->bind_select(stmt, &ctx);
    if (!status.ok()) {
      return Result(status);
    }

    std::unique_ptr<Executor> executor =
        build_scan_executor(ctx.table_info->oid, &ctx.table_info->schema,
                            std::move(ctx.predicate), exec_ctx, &status);
    if (!status.ok()) {
      return Result(status);
    }
    const Schema *input_schema = &ctx.table_info->schema;

    // Apply ORDER BY before projection. Sort keys are resolved against the full
    // table (input) schema, so ORDER BY can reference a column that is not in
    // the SELECT list.
    if (!stmt->order_by.empty()) {
      std::vector<SortKey> sort_keys;
      sort_keys.reserve(stmt->order_by.size());
      for (const auto &item : stmt->order_by) {
        bool found = false;
        for (size_t i = 0; i < input_schema->column_count(); i++) {
          if (input_schema->column(i).name() == item.column_name) {
            sort_keys.emplace_back(i, item.ascending);
            found = true;
            break;
          }
        }
        if (!found) {
          return Result(Status::InvalidArgument("ORDER BY column not found: " +
                                                item.column_name));
        }
      }
      executor = std::make_unique<SortExecutor>(
          exec_ctx, std::move(executor), input_schema, std::move(sort_keys));
    }

    const Schema *output_schema;
    if (ctx.select_all) {
      output_schema = input_schema;
    } else {
      auto proj = std::make_unique<ProjectionExecutor>(
          exec_ctx, std::move(executor), input_schema, ctx.column_indices);
      output_schema = &proj->output_schema();
      executor = std::move(proj);
    }

    if (stmt->limit.has_value() || stmt->offset.has_value()) {
      executor = std::make_unique<LimitExecutor>(
          exec_ctx, std::move(executor), stmt->limit, stmt->offset.value_or(0));
    }

    return collect_result(executor.get(), output_schema);
  }

  // General SELECT pipeline: joins -> residual filter -> aggregation ->
  // projection -> ORDER BY -> LIMIT. Name resolution is done here against the
  // combined schema; the optimizer chooses scan access paths and join methods.
  Result execute_select_query(SelectStatement *stmt,
                              ExecutorContext *exec_ctx) {
    // Owns intermediate (combined) schemas referenced by plan nodes/executors
    // for the duration of this call. Executors run to completion here, so a
    // local lifetime is sufficient.
    std::vector<std::unique_ptr<Schema>> owned_schemas;
    std::vector<std::shared_ptr<TableInfo>> pinned;

    // Resolve every base relation (driving table first) and accumulate the
    // combined schema plus, per column, the alias of its source relation.
    std::vector<TableRef> table_refs;
    table_refs.push_back(stmt->table);
    for (const auto &j : stmt->joins) {
      table_refs.push_back(j.table);
    }

    std::vector<ScanSpec> relations;
    std::vector<Column> combined_cols;
    std::vector<std::string> combined_tables;
    for (const auto &ref : table_refs) {
      std::shared_ptr<TableInfo> info = catalog_->get_table_shared(ref.table_name);
      if (info == nullptr) {
        return Result(Status::NotFound("Table not found: " + ref.table_name));
      }
      const std::string alias = ref.alias.empty() ? ref.table_name : ref.alias;
      relations.push_back(ScanSpec{info->oid, &info->schema, nullptr});
      for (size_t i = 0; i < info->schema.column_count(); ++i) {
        combined_cols.push_back(info->schema.column(i));
        combined_tables.push_back(alias);
      }
      pinned.push_back(std::move(info));
    }

    const bool has_joins = !stmt->joins.empty();

    // Single-table case: push the WHERE predicate into the scan so the
    // optimizer can pick an index. With joins, the predicate is applied as a
    // residual filter above the join instead.
    if (!has_joins && stmt->where_clause) {
      const std::vector<std::string> base_tables(
          combined_tables.begin(),
          combined_tables.begin() + static_cast<std::ptrdiff_t>(
                                        relations[0].schema->column_count()));
      Status s = resolve_expr(stmt->where_clause.get(), *relations[0].schema,
                              base_tables);
      if (!s.ok()) {
        return Result(s);
      }
      relations[0].filter = std::move(stmt->where_clause);
    }

    // Build join specs, resolving each ON condition against the combined schema
    // produced up to and including that join.
    std::vector<JoinSpec> joins;
    std::vector<Column> running_cols(
        combined_cols.begin(),
        combined_cols.begin() +
            static_cast<std::ptrdiff_t>(relations[0].schema->column_count()));
    std::vector<std::string> running_tables(
        combined_tables.begin(),
        combined_tables.begin() +
            static_cast<std::ptrdiff_t>(relations[0].schema->column_count()));
    size_t col_cursor = relations[0].schema->column_count();

    for (size_t ji = 0; ji < stmt->joins.size(); ++ji) {
      JoinClause &jc = stmt->joins[ji];
      const size_t left_width = running_cols.size();
      const size_t right_width = relations[ji + 1].schema->column_count();

      // Grow the combined schema/table map by the right relation's columns.
      for (size_t i = 0; i < right_width; ++i) {
        running_cols.push_back(combined_cols[col_cursor]);
        running_tables.push_back(combined_tables[col_cursor]);
        ++col_cursor;
      }
      owned_schemas.push_back(std::make_unique<Schema>(running_cols));
      const Schema *combined = owned_schemas.back().get();

      JoinSpec spec;
      spec.join_type = to_plan_join_type(jc.type);
      spec.output_schema = combined;

      if (jc.condition) {
        Status s = resolve_expr(jc.condition.get(), *combined, running_tables);
        if (!s.ok()) {
          return Result(s);
        }
        // Detect a single `left_col = right_col` equality so the optimizer may
        // consider a hash join; anything else uses the nested-loop condition.
        if (auto *cmp =
                dynamic_cast<ComparisonExpression *>(jc.condition.get());
            cmp != nullptr && cmp->cmp() == ComparisonType::EQUAL) {
          auto *l = dynamic_cast<const ColumnRefExpression *>(cmp->left());
          auto *r = dynamic_cast<const ColumnRefExpression *>(cmp->right());
          if (l != nullptr && r != nullptr) {
            const size_t li = l->column_index();
            const size_t ri = r->column_index();
            if (li < left_width && ri >= left_width) {
              spec.is_equi = true;
              spec.left_key_index = li;
              spec.right_key_index = ri - left_width;
            } else if (ri < left_width && li >= left_width) {
              spec.is_equi = true;
              spec.left_key_index = ri;
              spec.right_key_index = li - left_width;
            }
          }
        }
        spec.condition = std::move(jc.condition);
      }
      joins.push_back(std::move(spec));
    }

    // Cost-based physical plan for the scan/join core.
    std::unique_ptr<PlanNode> plan;
    Status planned = optimizer_->optimize(std::move(relations), std::move(joins),
                                          &plan);
    if (!planned.ok()) {
      return Result(planned);
    }
    std::unique_ptr<Executor> executor = build_plan_executor(plan.get(), exec_ctx);
    if (!executor) {
      return Result(Status::Internal("Failed to build query plan"));
    }
    const Schema *cur_schema = plan->output_schema();
    std::vector<std::string> cur_tables = running_tables;

    // Residual WHERE filter (join queries): apply after the join.
    if (has_joins && stmt->where_clause) {
      Status s = resolve_expr(stmt->where_clause.get(), *cur_schema, cur_tables);
      if (!s.ok()) {
        return Result(s);
      }
      executor = std::make_unique<FilterExecutor>(
          exec_ctx, std::move(executor), std::move(stmt->where_clause),
          cur_schema);
    }

    // Aggregation / GROUP BY.
    const bool has_aggregate = [&] {
      for (const auto &c : stmt->columns) {
        if (c.agg_func != AggregateFunc::NONE) {
          return true;
        }
      }
      return false;
    }();

    if (has_aggregate || !stmt->group_by.empty()) {
      return build_aggregation(stmt, std::move(executor), cur_schema,
                               cur_tables, exec_ctx);
    }

    // Projection (computed/aliased expressions, or a plain column list). A lone
    // `*` keeps the combined schema unchanged.
    const bool single_star =
        stmt->columns.size() == 1 && stmt->columns[0].is_star;
    if (!single_star) {
      std::vector<std::unique_ptr<Expression>> exprs;
      std::vector<Column> out_cols;
      for (auto &col : stmt->columns) {
        if (col.is_star) {
          // Expand `*` inline to every current column.
          for (size_t i = 0; i < cur_schema->column_count(); ++i) {
            auto ref = std::make_unique<ColumnRefExpression>(
                cur_schema->column(i).name());
            ref->set_column_index(i);
            ref->set_type(cur_schema->column(i).type());
            exprs.push_back(std::move(ref));
            out_cols.push_back(cur_schema->column(i));
          }
          continue;
        }

        std::unique_ptr<Expression> e;
        TypeId type = TypeId::INVALID;
        if (col.expression) {
          Status s = resolve_expr(col.expression.get(), *cur_schema, cur_tables);
          if (!s.ok()) {
            return Result(s);
          }
          type = col.expression->result_type();
          e = std::move(col.expression);
        } else {
          auto ref = std::make_unique<ColumnRefExpression>(col.column_name,
                                                           col.table_alias);
          Status s = resolve_expr(ref.get(), *cur_schema, cur_tables);
          if (!s.ok()) {
            return Result(s);
          }
          type = ref->result_type();
          e = std::move(ref);
        }
        std::string name = !col.alias.empty()
                               ? col.alias
                               : (col.column_name.empty() ? "expr"
                                                          : col.column_name);
        out_cols.emplace_back(name, type);
        exprs.push_back(std::move(e));
      }

      auto proj = std::make_unique<ProjectionExecutor>(
          exec_ctx, std::move(executor), cur_schema, std::move(exprs),
          std::move(out_cols));
      cur_schema = &proj->output_schema();
      executor = std::move(proj);
    }

    return finalize_query(std::move(executor), cur_schema, stmt, exec_ctx);
  }

  // Build the aggregation stage plus a final projection that reorders the
  // grouped output into the SELECT list and applies aliases.
  Result build_aggregation(SelectStatement *stmt,
                           std::unique_ptr<Executor> child,
                           const Schema *input_schema,
                           const std::vector<std::string> &input_tables,
                           ExecutorContext *exec_ctx) {
    // Resolve GROUP BY columns to input indices.
    std::vector<size_t> group_by_indices;
    group_by_indices.reserve(stmt->group_by.size());
    for (const auto &name : stmt->group_by) {
      int idx = input_schema->get_column_index(name);
      if (idx < 0) {
        return Result(Status::InvalidArgument("GROUP BY column not found: " +
                                              name));
      }
      group_by_indices.push_back(static_cast<size_t>(idx));
    }

    // Build the aggregate list and remember, per SELECT item, its group-by
    // position (nullopt marks an aggregate item, resolved positionally below).
    // The aggregation output lays out group columns first, then aggregates.
    std::vector<AggregateExpression> aggregates;
    std::vector<std::optional<size_t>> select_group_pos;

    for (auto &col : stmt->columns) {
      if (col.is_star) {
        return Result(Status::NotSupported(
            "SELECT * cannot be combined with aggregation"));
      }
      if (col.agg_func != AggregateFunc::NONE) {
        size_t arg_index = 0;
        if (col.agg_func != AggregateFunc::COUNT_STAR) {
          auto *ref = dynamic_cast<ColumnRefExpression *>(col.expression.get());
          if (ref == nullptr) {
            return Result(Status::NotSupported(
                "aggregate argument must be a column reference"));
          }
          Status s = resolve_expr(ref, *input_schema, input_tables);
          if (!s.ok()) {
            return Result(s);
          }
          arg_index = ref->column_index();
        }
        std::string name = col.alias.empty() ? "agg" : col.alias;
        aggregates.emplace_back(to_exec_aggregate(col.agg_func), arg_index,
                                name);
        select_group_pos.push_back(std::nullopt);
      } else {
        // A bare column in an aggregate query must be a GROUP BY column.
        int pos = -1;
        for (size_t g = 0; g < stmt->group_by.size(); ++g) {
          if (stmt->group_by[g] == col.column_name) {
            pos = static_cast<int>(g);
            break;
          }
        }
        if (pos < 0) {
          return Result(Status::InvalidArgument(
              "SELECT column must appear in GROUP BY or an aggregate: " +
              col.column_name));
        }
        select_group_pos.push_back(static_cast<size_t>(pos));
      }
    }

    auto agg_exec = std::make_unique<AggregationExecutor>(
        exec_ctx, std::move(child), input_schema, group_by_indices,
        std::move(aggregates));
    const Schema *agg_schema = &agg_exec->output_schema();

    // Final projection into SELECT order with alias names.
    std::vector<std::unique_ptr<Expression>> exprs;
    std::vector<Column> out_cols;
    size_t agg_counter = 0;
    const size_t group_count = group_by_indices.size();
    for (size_t c = 0; c < stmt->columns.size(); ++c) {
      const auto &col = stmt->columns[c];
      const size_t out_index = select_group_pos[c].has_value()
                                   ? *select_group_pos[c]
                                   : group_count + agg_counter++;
      auto ref = std::make_unique<ColumnRefExpression>(
          agg_schema->column(out_index).name());
      ref->set_column_index(out_index);
      ref->set_type(agg_schema->column(out_index).type());
      std::string name =
          !col.alias.empty()
              ? col.alias
              : (col.column_name.empty() ? agg_schema->column(out_index).name()
                                         : col.column_name);
      out_cols.emplace_back(name, agg_schema->column(out_index).type());
      exprs.push_back(std::move(ref));
    }

    auto proj = std::make_unique<ProjectionExecutor>(
        exec_ctx, std::move(agg_exec), agg_schema, std::move(exprs),
        std::move(out_cols));
    const Schema *out_schema = &proj->output_schema();

    return finalize_query(std::move(proj), out_schema, stmt, exec_ctx);
  }

  Result execute_insert(InsertStatement *stmt, ExecutorContext *exec_ctx) {
    BoundInsertContext ctx;
    Status status = binder_->bind_insert(stmt, &ctx);
    if (!status.ok()) {
      return Result(status);
    }

    // Convert parsed values to Tuples
    std::vector<Tuple> tuples;
    for (const auto &row : stmt->values) {
      std::vector<TupleValue> values;
      values.reserve(ctx.table_info->schema.column_count());

      // Initialize with NULLs
      for (size_t i = 0; i < ctx.table_info->schema.column_count(); i++) {
        values.push_back(TupleValue::null());
      }

      // Fill in provided values with schema-aware type conversion
      for (size_t i = 0; i < row.size(); i++) {
        size_t col_idx = ctx.column_indices[i];
        TypeId col_type = ctx.table_info->schema.column(col_idx).type();
        values[col_idx] = parsed_value_to_tuple_value(row[i], col_type);
      }

      tuples.emplace_back(std::move(values), ctx.table_info->schema);
    }

    InsertExecutor insert(exec_ctx, ctx.table_info->table_heap,
                          std::move(tuples), ctx.table_info->oid);
    insert.init();
    (void)insert.next();

    if (!insert.status().ok()) {
      return Result(insert.status());
    }

    statistics_->on_rows_inserted(ctx.table_info->oid, insert.rows_inserted());
    return Result(insert.rows_inserted());
  }

  Result execute_update(UpdateStatement *stmt, ExecutorContext *exec_ctx) {
    BoundUpdateContext ctx;
    Status status = binder_->bind_update(stmt, &ctx);
    if (!status.ok()) {
      return Result(status);
    }

    // Create child scan with predicate
    auto child = std::make_unique<SeqScanExecutor>(
        exec_ctx, ctx.table_info->table_heap, &ctx.table_info->schema,
        std::move(ctx.predicate));

    UpdateExecutor update(exec_ctx, std::move(child),
                          ctx.table_info->table_heap, &ctx.table_info->schema,
                          std::move(ctx.column_indices), std::move(ctx.values),
                          ctx.table_info->oid);
    update.init();
    (void)update.next();

    if (!update.status().ok()) {
      return Result(update.status());
    }

    return Result(update.rows_updated());
  }

  Result execute_delete(DeleteStatement *stmt, ExecutorContext *exec_ctx) {
    BoundDeleteContext ctx;
    Status status = binder_->bind_delete(stmt, &ctx);
    if (!status.ok()) {
      return Result(status);
    }

    // Create child scan with predicate
    auto child = std::make_unique<SeqScanExecutor>(
        exec_ctx, ctx.table_info->table_heap, &ctx.table_info->schema,
        std::move(ctx.predicate));

    DeleteExecutor del(exec_ctx, std::move(child), ctx.table_info->table_heap,
                       ctx.table_info->oid);
    del.init();
    (void)del.next();

    if (!del.status().ok()) {
      return Result(del.status());
    }

    statistics_->on_rows_deleted(ctx.table_info->oid, del.rows_deleted());
    return Result(del.rows_deleted());
  }

  Result execute_create_table(CreateTableStatement *stmt) {
    // Build schema from column definitions
    std::vector<Column> columns;
    for (const auto &col_def : stmt->columns) {
      columns.emplace_back(col_def.name, col_def.type, col_def.length);
    }
    Schema schema(std::move(columns));

    Status status = catalog_->create_table(stmt->table_name, schema);
    if (!status.ok()) {
      return Result(status);
    }

    if (auto *table_info = catalog_->get_table(stmt->table_name)) {
      statistics_->on_table_created(table_info->oid);
    }
    return Result(size_t(0)); // 0 rows affected
  }

  Result execute_drop_table(DropTableStatement *stmt) {
    oid_t table_oid = catalog_->get_table_oid(stmt->table_name);

    // Snapshot the heap's page ids before the drop reclaims them: nothing
    // can recover them afterwards.
    std::unordered_set<page_id_t> dropped_pages;
    if (auto info = catalog_->get_table_shared(stmt->table_name);
        info && info->table_heap) {
      dropped_pages = info->table_heap->page_ids();
    }

    // Logical drop: remove the table (and its indexes) from the catalog and
    // DISCARD its heap pages from the buffer pool, but keep their ids ALLOCATED
    // for now — deallocation is deferred until the checkpoint below succeeds
    // (crash-safety F2). Discarding drops the dirty heap bytes without flushing
    // them, so the checkpoint's page flush never persists the dropped rows onto
    // a page a reuse will inherit.
    Status status =
        catalog_->drop_table(stmt->table_name, /*deallocate_heap_pages=*/false);
    if (!status.ok()) {
      return Result(status);
    }

    // Version chains keyed by RIDs on the dropped pages — a later table reusing
    // a page must not inherit visibility decisions from dropped rows.
    version_store_->purge_pages(dropped_pages);

    // The table is gone from the catalog regardless of how the checkpoint
    // below fares, so retire its statistics on every path from here on.
    if (table_oid != INVALID_OID) {
      statistics_->on_table_dropped(table_oid);
    }

    // Advance the redo anchor past the dropped table's WAL records. Redo
    // replays records in LSN order onto pages by id; without a boundary, a
    // reused page's fresh content would first be rebuilt from the dropped
    // table's records (whose page LSNs are gone with the zeroed page) and the
    // new table's own inserts would then collide with the resurrected slots. A
    // checkpoint HERE advances the anchor past every record that targeted the
    // freed pages, exactly like the startup checkpoint does after recovery. The
    // writer barrier quiesces concurrent appends for the anchor capture.
    if (wal_manager_) {
      RecoveryManager recovery(buffer_pool_, wal_manager_, disk_manager_);
      Status checkpointed = recovery.create_checkpoint(
          txn_manager_->get_active_txn_ids(),
          &txn_manager_->checkpoint_barrier());
      if (!checkpointed.ok()) {
        // The anchor did not advance, so the freed ids must NOT be reused: a
        // reuse-then-crash would replay the dropped table's inserts into the
        // new page. Leave the ids allocated (leaked — safe, an unreused id can
        // never resurrect the dropped rows into a live table) and fail loudly
        // instead of silently degrading to the original page-reuse corruption.
        LOG_ERROR("Post-drop checkpoint failed; leaking {} freed page(s) to "
                  "stay crash-safe rather than reuse them: {}",
                  dropped_pages.size(), checkpointed.message());
        return Result(Status::IOError(
            "DROP TABLE: post-drop checkpoint failed; the table was dropped "
            "but its pages were leaked to stay crash-safe: " +
            std::string(checkpointed.message())));
      }
    }

    // Checkpoint succeeded (or WAL is disabled and there is no recovery to
    // anchor): the ids are now safe to return to the free list for reuse.
    for (page_id_t page_id : dropped_pages) {
      disk_manager_->deallocate_page(page_id);
    }

    return Result(size_t(0)); // 0 rows affected
  }

  Result execute_explain(ExplainStatement *stmt) {
    if (!stmt || !stmt->inner_statement) {
      return Result(Status::InvalidArgument("Invalid EXPLAIN statement"));
    }

    // Currently only support SELECT
    if (stmt->inner_statement->type() != StatementType::SELECT) {
      return Result(Status::NotSupported("EXPLAIN only supports SELECT"));
    }

    auto* select = dynamic_cast<SelectStatement*>(stmt->inner_statement.get());

    // Bind to get table info
    BoundSelectContext ctx;
    Status status = binder_->bind_select(select, &ctx);
    if (!status.ok()) {
      return Result(status);
    }

    // Generate query plan description
    std::vector<std::string> plan_lines;
    plan_lines.push_back("Query Plan:");

    // Check if index can be used
    auto selection = index_selector_->select_access_method(
        ctx.table_info->oid, ctx.predicate.get());

    // Describe access method
    if (selection.use_index) {
      std::string access = "-> Index Scan";
      switch (selection.scan_type) {
        case IndexScanPlanNode::ScanType::POINT_LOOKUP:
          access += " (Point Lookup)";
          break;
        case IndexScanPlanNode::ScanType::RANGE_SCAN:
          access += " (Range Scan)";
          break;
        case IndexScanPlanNode::ScanType::FULL_SCAN:
          access += " (Full Index Scan)";
          break;
      }
      plan_lines.push_back(access);
      plan_lines.push_back("   Index Cost: " + std::to_string(selection.index_cost));
    } else {
      plan_lines.push_back("-> Sequential Scan on " + ctx.table_info->name);
      plan_lines.push_back("   SeqScan Cost: " + std::to_string(selection.seq_scan_cost));
    }

    // Describe predicate if present
    if (ctx.predicate) {
      plan_lines.push_back("   Filter: (predicate)");
    }

    // Describe ORDER BY if present
    if (!select->order_by.empty()) {
      plan_lines.push_back("-> Sort");
      for (const auto& key : select->order_by) {
        plan_lines.push_back("   Key: " + key.column_name +
                            (key.ascending ? " ASC" : " DESC"));
      }
    }

    // Describe LIMIT if present
    if (select->limit.has_value()) {
      plan_lines.push_back("-> Limit: " + std::to_string(*select->limit));
    }

    // Cardinality estimate
    size_t estimated_rows = statistics_->table_cardinality(ctx.table_info->oid);
    if (ctx.predicate) {
      double sel = statistics_->estimate_selectivity(ctx.table_info->oid,
                                                      ctx.predicate.get());
      double estimated_rows_d = static_cast<double>(estimated_rows);
      estimated_rows = static_cast<size_t>(estimated_rows_d * sel);
    }
    plan_lines.push_back("Estimated Rows: " + std::to_string(estimated_rows));

    // Build result rows
    std::vector<Row> rows;
    std::vector<std::string> column_names = {"QUERY PLAN"};
    for (const auto& line : plan_lines) {
      rows.emplace_back(std::vector<Value>{Value(line)}, column_names);
    }

    return Result(std::move(rows), std::move(column_names));
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Helper Functions
  // ─────────────────────────────────────────────────────────────────────────

  static Value tuple_value_to_value(const TupleValue &tv) {
    if (tv.is_null())
      return Value();
    if (tv.is_bool())
      return Value(tv.as_bool());
    // Narrow integer types widen to the public Value's int32 representation.
    if (tv.is_tinyint())
      return Value(static_cast<int32_t>(tv.as_tinyint()));
    if (tv.is_smallint())
      return Value(static_cast<int32_t>(tv.as_smallint()));
    if (tv.is_integer())
      return Value(tv.as_integer());
    if (tv.is_bigint())
      return Value(tv.as_bigint());
    // FLOAT widens to the public Value's double representation.
    if (tv.is_float())
      return Value(static_cast<double>(tv.as_float()));
    if (tv.is_double())
      return Value(tv.as_double());
    if (tv.is_string())
      return Value(tv.as_string());
    return Value(); // Fallback to NULL
  }

  static TupleValue parsed_value_to_tuple_value(const InsertValue &pv,
                                                TypeId target_type) {
    if (std::holds_alternative<std::monostate>(pv)) {
      return TupleValue::null();
    } else if (std::holds_alternative<bool>(pv)) {
      return TupleValue(std::get<bool>(pv));
    } else if (std::holds_alternative<int64_t>(pv)) {
      int64_t val = std::get<int64_t>(pv);
      // Convert to appropriate integer type based on target type
      switch (target_type) {
      case TypeId::TINYINT:
        return TupleValue(static_cast<int8_t>(val));
      case TypeId::SMALLINT:
        return TupleValue(static_cast<int16_t>(val));
      case TypeId::INTEGER:
        return TupleValue(static_cast<int32_t>(val));
      case TypeId::BIGINT:
      default:
        return TupleValue(val);
      }
    } else if (std::holds_alternative<double>(pv)) {
      double val = std::get<double>(pv);
      if (target_type == TypeId::FLOAT) {
        return TupleValue(static_cast<float>(val));
      }
      return TupleValue(val);
    } else if (std::holds_alternative<std::string>(pv)) {
      return TupleValue(std::get<std::string>(pv));
    }
    return TupleValue::null();
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Transaction & Lifecycle
  // ─────────────────────────────────────────────────────────────────────────

  Status begin_transaction() {
    if (!is_open_) {
      return Status::InvalidArgument("Database is not open");
    }
    std::lock_guard<std::mutex> lock(session_mutex_);
    const auto tid = std::this_thread::get_id();
    if (thread_txns_.count(tid) != 0) {
      return Status::Error("Already in a transaction");
    }
    Transaction *txn = txn_manager_->begin();
    if (txn == nullptr) {
      return Status::Internal("Failed to begin transaction");
    }
    thread_txns_[tid] = txn;
    return Status::Ok();
  }

  Status commit() {
    SessionState state = unbind_thread_txn();
    if (!state.bound) {
      return Status::Error("No active transaction");
    }
    if (state.txn == nullptr) {
      // The transaction was already aborted by a failed statement; there is
      // nothing to make durable. Clear the binding and tell the caller.
      return Status::Aborted(
          "Cannot commit: transaction was aborted and has been rolled back");
    }
    txn_manager_->commit(state.txn);
    return Status::Ok();
  }

  Status rollback() {
    SessionState state = unbind_thread_txn();
    if (!state.bound) {
      return Status::Error("No active transaction");
    }
    if (state.txn != nullptr) {
      txn_manager_->abort(state.txn);
    }
    // A failed binding was already aborted; clearing it completes the
    // rollback the caller owed.
    return Status::Ok();
  }

  bool in_transaction() const noexcept { return session_state().bound; }

  Status close() {
    Status result = Status::Ok();
    if (is_open_) {
      LOG_INFO("Closing database: {}", path_);
      // Clean shutdown: WAL first, then every dirty page. Surface the first
      // failure instead of swallowing it — a caller that checks close() must
      // be able to tell durability was not achieved.
      if (wal_manager_) {
        result = wal_manager_->flush();
      }
      if (buffer_pool_) {
        Status flushed = buffer_pool_->flush_all_pages();
        if (result.ok() && !flushed.ok()) {
          result = flushed;
        }
      }
      is_open_ = false;
    }
    return result;
  }

  bool is_open() const noexcept { return is_open_; }
  std::string_view path() const noexcept { return path_; }

  // Test-only seam: hands out the internal Catalog pointer (see header).
  Catalog *catalog_for_testing() noexcept { return catalog_.get(); }

private:
  /// Result of looking up the calling thread's explicit-transaction binding.
  struct SessionState {
    bool bound = false;          ///< an explicit transaction is open (or failed)
    Transaction *txn = nullptr;  ///< live transaction; null when failed
  };

  [[nodiscard]] SessionState session_state() const {
    std::lock_guard<std::mutex> lock(session_mutex_);
    auto it = thread_txns_.find(std::this_thread::get_id());
    if (it == thread_txns_.end()) {
      return {};
    }
    return {true, it->second};
  }

  /// Mark the calling thread's binding failed (its transaction was aborted);
  /// the binding survives until rollback/commit so later statements error.
  void mark_thread_txn_failed() {
    std::lock_guard<std::mutex> lock(session_mutex_);
    auto it = thread_txns_.find(std::this_thread::get_id());
    if (it != thread_txns_.end()) {
      it->second = nullptr;
    }
  }

  /// Remove the calling thread's binding, returning its state before removal.
  SessionState unbind_thread_txn() {
    std::lock_guard<std::mutex> lock(session_mutex_);
    auto it = thread_txns_.find(std::this_thread::get_id());
    if (it == thread_txns_.end()) {
      return {};
    }
    SessionState state{true, it->second};
    thread_txns_.erase(it);
    return state;
  }

  std::string path_;
  std::string open_error_;
  bool strict_mode_ = false;
  bool is_open_ = false;

  // Storage layer
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_;

  // Durability & concurrency control
  std::shared_ptr<WALManager> wal_manager_;
  std::unique_ptr<LockManager> lock_manager_;
  std::shared_ptr<MVCCManager> mvcc_;
  std::shared_ptr<VersionStore> version_store_;
  std::unique_ptr<TransactionManager> txn_manager_;

  // Catalog & Binder
  std::shared_ptr<Catalog> catalog_;
  std::unique_ptr<Binder> binder_;

  // Optimizer
  std::shared_ptr<Statistics> statistics_;
  std::shared_ptr<CostModel> cost_model_;
  std::unique_ptr<IndexSelector> index_selector_;
  std::unique_ptr<Optimizer> optimizer_;

  // Per-thread explicit transactions ("a connection is a thread"). A mapped
  // null value is a FAILED binding: the transaction was aborted by a
  // statement error or write-write conflict, and the entry stays until the
  // caller issues rollback (or commit, which errors and clears) — statements
  // meanwhile are rejected instead of silently autocommitting.
  mutable std::mutex session_mutex_;
  std::unordered_map<std::thread::id, Transaction *> thread_txns_;
};

Database::Database(const std::string &path, const DatabaseOptions &options)
    : impl_(std::make_unique<DatabaseImpl>(path, options)) {}

Database::Database(const std::string &path,
                   std::shared_ptr<DiskManager> disk_manager,
                   const DatabaseOptions &options)
    : impl_(std::make_unique<DatabaseImpl>(path, options,
                                           std::move(disk_manager))) {}

Database::~Database() = default;

Database::Database(Database &&) noexcept = default;
Database &Database::operator=(Database &&) noexcept = default;

Result Database::execute(std::string_view sql) { return impl_->execute(sql); }

Status Database::begin_transaction() { return impl_->begin_transaction(); }

Status Database::commit() { return impl_->commit(); }

Status Database::rollback() { return impl_->rollback(); }

bool Database::in_transaction() const noexcept {
  return impl_->in_transaction();
}

Status Database::close() { return impl_->close(); }

bool Database::is_open() const noexcept { return impl_->is_open(); }

std::string_view Database::path() const noexcept { return impl_->path(); }

Catalog *Database::catalog_for_testing() noexcept {
  return impl_->catalog_for_testing();
}

} // namespace entropy
