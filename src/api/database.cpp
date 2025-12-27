/**
 * @file database.cpp
 * @brief Database class implementation with SQL execution
 */

#include "entropy/database.hpp"

#include <utility>

#include "catalog/catalog.hpp"
#include "common/logger.hpp"
#include "execution/delete_executor.hpp"
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

namespace entropy {

class DatabaseImpl {
public:
  explicit DatabaseImpl(const std::string &path, const DatabaseOptions &options)
      : path_(path), is_open_(true) {
    Logger::init();
    LOG_INFO("Opening database: {}", path);

    // Initialize storage layer
    disk_manager_ = std::make_shared<DiskManager>(path);
    buffer_pool_ = std::make_shared<BufferPoolManager>(options.buffer_pool_size,
                                                       disk_manager_);

    // Initialize catalog
    catalog_ = std::make_shared<Catalog>(buffer_pool_);
    binder_ = std::make_unique<Binder>(catalog_.get());

    // Initialize optimizer components
    statistics_ = std::make_shared<Statistics>(catalog_);
    cost_model_ = std::make_shared<CostModel>(statistics_);
    index_selector_ =
        std::make_unique<IndexSelector>(catalog_, statistics_, cost_model_);
  }

  ~DatabaseImpl() { close(); }

  Result execute(std::string_view sql) {
    // Parse SQL
    Parser parser(sql);
    std::unique_ptr<Statement> stmt;
    Status status = parser.parse(&stmt);
    if (!status.ok()) {
      return Result(status);
    }

    // Execute based on statement type
    switch (stmt->type()) {
    case StatementType::SELECT:
      return execute_select(dynamic_cast<SelectStatement *>(stmt.get()));
    case StatementType::INSERT:
      return execute_insert(dynamic_cast<InsertStatement *>(stmt.get()));
    case StatementType::UPDATE:
      return execute_update(dynamic_cast<UpdateStatement *>(stmt.get()));
    case StatementType::DELETE_STMT:
      return execute_delete(dynamic_cast<DeleteStatement *>(stmt.get()));
    case StatementType::CREATE_TABLE:
      return execute_create_table(
          dynamic_cast<CreateTableStatement *>(stmt.get()));
    case StatementType::DROP_TABLE:
      return execute_drop_table(dynamic_cast<DropTableStatement *>(stmt.get()));
    case StatementType::EXPLAIN:
      return execute_explain(dynamic_cast<ExplainStatement *>(stmt.get()));
    default:
      return Result(Status::NotSupported("Unsupported statement type"));
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Statement Execution
  // ─────────────────────────────────────────────────────────────────────────

  Result execute_select(SelectStatement *stmt) {
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
      // Get index info
      auto *index_info = catalog_->get_index_for_column(
          ctx.table_info->oid, static_cast<column_id_t>(selection.index_oid));

      if (index_info && index_info->index) {
        // Use IndexScan
        switch (selection.scan_type) {
        case IndexScanPlanNode::ScanType::POINT_LOOKUP:
          scan = std::make_unique<IndexScanExecutor>(
              nullptr, index_info->index.get(),
              ctx.table_info->table_heap.get(), &ctx.table_info->schema,
              *selection.start_key);
          break;
        case IndexScanPlanNode::ScanType::RANGE_SCAN:
          scan = std::make_unique<IndexScanExecutor>(
              nullptr, index_info->index.get(),
              ctx.table_info->table_heap.get(), &ctx.table_info->schema,
              selection.start_key.value_or(
                  std::numeric_limits<BPTreeKey>::min()),
              selection.end_key.value_or(
                  std::numeric_limits<BPTreeKey>::max()));
          break;
        case IndexScanPlanNode::ScanType::FULL_SCAN:
          scan = std::make_unique<IndexScanExecutor>(
              nullptr, index_info->index.get(),
              ctx.table_info->table_heap.get(), &ctx.table_info->schema);
          break;
        }
      }
    }

    // Fallback to SeqScan
    if (!scan) {
      scan = std::make_unique<SeqScanExecutor>(
          nullptr, ctx.table_info->table_heap, &ctx.table_info->schema,
          std::move(ctx.predicate));
    }

    // If not SELECT *, create projection
    std::unique_ptr<Executor> executor;
    const Schema *output_schema;
    if (ctx.select_all) {
      output_schema = &ctx.table_info->schema;
      executor = std::move(scan);
    } else {
      auto proj = std::make_unique<ProjectionExecutor>(nullptr, std::move(scan),
                                                       &ctx.table_info->schema,
                                                       ctx.column_indices);
      output_schema = &proj->output_schema();
      executor = std::move(proj);
    }

    // Add ORDER BY if specified
    if (!stmt->order_by.empty()) {
      std::vector<SortKey> sort_keys;
      for (const auto &item : stmt->order_by) {
        // Find column index by name
        for (size_t i = 0; i < output_schema->column_count(); i++) {
          if (output_schema->column(i).name() == item.column_name) {
            sort_keys.emplace_back(i, item.ascending);
            break;
          }
        }
      }
      if (!sort_keys.empty()) {
        executor = std::make_unique<SortExecutor>(
            nullptr, std::move(executor), output_schema, std::move(sort_keys));
      }
    }

    // Add LIMIT/OFFSET if specified
    if (stmt->limit.has_value() || stmt->offset.has_value()) {
      executor = std::make_unique<LimitExecutor>(
          nullptr, std::move(executor), stmt->limit, stmt->offset.value_or(0));
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

  Result execute_insert(InsertStatement *stmt) {
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

    InsertExecutor insert(nullptr, ctx.table_info->table_heap,
                          &ctx.table_info->schema, std::move(tuples));
    insert.init();
    (void)insert.next();

    statistics_->on_rows_inserted(ctx.table_info->oid, insert.rows_inserted());
    return Result(insert.rows_inserted());
  }

  Result execute_update(UpdateStatement *stmt) {
    BoundUpdateContext ctx;
    Status status = binder_->bind_update(stmt, &ctx);
    if (!status.ok()) {
      return Result(status);
    }

    // Create child scan with predicate
    auto child = std::make_unique<SeqScanExecutor>(
        nullptr, ctx.table_info->table_heap, &ctx.table_info->schema,
        std::move(ctx.predicate));

    UpdateExecutor update(nullptr, std::move(child), ctx.table_info->table_heap,
                          &ctx.table_info->schema,
                          std::move(ctx.column_indices), std::move(ctx.values));
    update.init();
    (void)update.next();

    return Result(update.rows_updated());
  }

  Result execute_delete(DeleteStatement *stmt) {
    BoundDeleteContext ctx;
    Status status = binder_->bind_delete(stmt, &ctx);
    if (!status.ok()) {
      return Result(status);
    }

    // Create child scan with predicate
    auto child = std::make_unique<SeqScanExecutor>(
        nullptr, ctx.table_info->table_heap, &ctx.table_info->schema,
        std::move(ctx.predicate));

    DeleteExecutor del(nullptr, std::move(child), ctx.table_info->table_heap);
    del.init();
    (void)del.next();

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
    Status status = catalog_->drop_table(stmt->table_name);
    if (!status.ok()) {
      return Result(status);
    }

    if (table_oid != INVALID_OID) {
      statistics_->on_table_dropped(table_oid);
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
      estimated_rows = static_cast<size_t>(estimated_rows * sel);
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
    if (tv.is_integer())
      return Value(tv.as_integer());
    if (tv.is_bigint())
      return Value(tv.as_bigint());
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
    if (in_transaction_) {
      return Status::Error("Already in a transaction");
    }
    in_transaction_ = true;
    return Status::Ok();
  }

  Status commit() {
    if (!in_transaction_) {
      return Status::Error("No active transaction");
    }
    in_transaction_ = false;
    return Status::Ok();
  }

  Status rollback() {
    if (!in_transaction_) {
      return Status::Error("No active transaction");
    }
    in_transaction_ = false;
    return Status::Ok();
  }

  bool in_transaction() const noexcept { return in_transaction_; }

  void close() {
    if (is_open_) {
      LOG_INFO("Closing database: {}", path_);
      buffer_pool_->flush_all_pages();
      is_open_ = false;
    }
  }

  bool is_open() const noexcept { return is_open_; }
  std::string_view path() const noexcept { return path_; }

private:
  std::string path_;
  bool is_open_ = false;
  bool in_transaction_ = false;

  // Storage layer
  std::shared_ptr<DiskManager> disk_manager_;
  std::shared_ptr<BufferPoolManager> buffer_pool_;

  // Catalog & Binder
  std::shared_ptr<Catalog> catalog_;
  std::unique_ptr<Binder> binder_;

  // Optimizer
  std::shared_ptr<Statistics> statistics_;
  std::shared_ptr<CostModel> cost_model_;
  std::unique_ptr<IndexSelector> index_selector_;
};

Database::Database(const std::string &path, const DatabaseOptions &options)
    : impl_(std::make_unique<DatabaseImpl>(path, options)) {}

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

void Database::close() { impl_->close(); }

bool Database::is_open() const noexcept { return impl_->is_open(); }

std::string_view Database::path() const noexcept { return impl_->path(); }

} // namespace entropy
