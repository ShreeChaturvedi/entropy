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

#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"
#include "execution/delete_executor.hpp"
#include "execution/executor_context.hpp"
#include "execution/index_scan_executor.hpp"
#include "execution/insert_executor.hpp"
#include "execution/limit_executor.hpp"
#include "execution/projection.hpp"
#include "execution/seq_scan_executor.hpp"
#include "execution/sort_executor.hpp"
#include "execution/update_executor.hpp"
#include "optimizer/cost_model.hpp"
#include "optimizer/index_selector.hpp"
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

  Result execute_select(SelectStatement *stmt, ExecutorContext *exec_ctx) {
    if (!stmt->group_by.empty()) {
      return Result(Status::NotSupported(
          "GROUP BY is not yet supported for execution"));
    }

    BoundSelectContext ctx;
    Status status = binder_->bind_select(stmt, &ctx);
    if (!status.ok()) {
      return Result(status);
    }

    // Use optimizer to select access method
    std::unique_ptr<Executor> scan;

    // Try to use IndexScan if predicate matches an indexed column
    auto selection = index_selector_->select_access_method(ctx.table_info->oid,
                                                           ctx.predicate.get());

    if (selection.use_index) {
      // Resolve by index OID — never cast OID to column_id_t
      auto *index_info = catalog_->get_index_by_oid(selection.index_oid);

      if (index_info && index_info->index) {
        // Use IndexScan
        switch (selection.scan_type) {
        case IndexScanPlanNode::ScanType::POINT_LOOKUP:
          scan = std::make_unique<IndexScanExecutor>(
              exec_ctx, index_info->index.get(),
              ctx.table_info->table_heap.get(), &ctx.table_info->schema,
              *selection.start_key);
          break;
        case IndexScanPlanNode::ScanType::RANGE_SCAN:
          scan = std::make_unique<IndexScanExecutor>(
              exec_ctx, index_info->index.get(),
              ctx.table_info->table_heap.get(), &ctx.table_info->schema,
              selection.start_key.value_or(
                  std::numeric_limits<BPTreeKey>::min()),
              selection.end_key.value_or(
                  std::numeric_limits<BPTreeKey>::max()));
          break;
        case IndexScanPlanNode::ScanType::FULL_SCAN:
          scan = std::make_unique<IndexScanExecutor>(
              exec_ctx, index_info->index.get(),
              ctx.table_info->table_heap.get(), &ctx.table_info->schema);
          break;
        }
      }
    }

    // Fallback to SeqScan
    if (!scan) {
      scan = std::make_unique<SeqScanExecutor>(
          exec_ctx, ctx.table_info->table_heap, &ctx.table_info->schema,
          std::move(ctx.predicate));
    }

    std::unique_ptr<Executor> executor = std::move(scan);
    const Schema *input_schema = &ctx.table_info->schema;

    // Apply ORDER BY before projection. Sort keys are resolved against the full
    // table (input) schema, so ORDER BY can reference a column that is not in
    // the SELECT list. Resolving against the post-projection schema instead
    // would silently drop such keys and leave the output unsorted.
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

    // If not SELECT *, create projection (after ORDER BY, which may reference
    // columns that are not projected).
    const Schema *output_schema;
    if (ctx.select_all) {
      output_schema = input_schema;
    } else {
      auto proj = std::make_unique<ProjectionExecutor>(
          exec_ctx, std::move(executor), input_schema, ctx.column_indices);
      output_schema = &proj->output_schema();
      executor = std::move(proj);
    }

    // Add LIMIT/OFFSET if specified
    if (stmt->limit.has_value() || stmt->offset.has_value()) {
      executor = std::make_unique<LimitExecutor>(
          exec_ctx, std::move(executor), stmt->limit, stmt->offset.value_or(0));
    }

    // Execute and collect results
    executor->init();
    std::vector<Row> rows;
    std::vector<std::string> column_names;

    // Get column names
    for (size_t i = 0; i < output_schema->column_count(); i++) {
      column_names.push_back(output_schema->column(i).name());
    }

    // Collect tuples
    while (auto tuple = executor->next()) {
      std::vector<Value> values;
      for (size_t i = 0; i < output_schema->column_count(); i++) {
        TupleValue tv =
            tuple->get_value(*output_schema, static_cast<uint32_t>(i));
        values.push_back(tuple_value_to_value(tv));
      }
      rows.emplace_back(std::move(values), column_names);
    }

    return Result(std::move(rows), std::move(column_names));
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

// Moving leaves the source's impl_ null (a valid, empty state). The noexcept
// accessors below all tolerate a null impl_, so a moved-from Database answers
// its queries without dereferencing through a null pimpl.
Database::Database(Database &&) noexcept = default;
Database &Database::operator=(Database &&) noexcept = default;

Result Database::execute(std::string_view sql) { return impl_->execute(sql); }

Status Database::begin_transaction() { return impl_->begin_transaction(); }

Status Database::commit() { return impl_->commit(); }

Status Database::rollback() { return impl_->rollback(); }

bool Database::in_transaction() const noexcept {
  return impl_ && impl_->in_transaction();
}

Status Database::close() { return impl_->close(); }

bool Database::is_open() const noexcept { return impl_ && impl_->is_open(); }

std::string_view Database::path() const noexcept {
  return impl_ ? impl_->path() : std::string_view{};
}

Catalog *Database::catalog_for_testing() noexcept {
  return impl_ ? impl_->catalog_for_testing() : nullptr;
}

} // namespace entropy
