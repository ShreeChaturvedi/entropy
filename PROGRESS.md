# Entropy Database - Development Progress

> **Purpose**: Track development progress across all phases. Update this document after completing any significant work. This is the single source of truth for project status.

---

## Quick Status

| Phase | Status | Progress |
|-------|--------|----------|
| Phase 1: Storage Engine | 🟢 Complete | 100% |
| Phase 2: Transactions | 🟢 Complete | ~95% |
| Phase 3: Query Processing | 🟢 Complete | 100% |
| Phase 4: Optimization | 🟢 Complete | 100% |

**Overall Progress**: ~98%

**Last Updated**: 2024-12-18 - Hash Index + Page Compression (352 tests passing)

---

## Phase 1: Storage Engine (Foundation)

### 1.1 Core Infrastructure
- [x] **Project Setup**
  - [x] CMake build system configured
  - [x] Directory structure created
  - [x] Google Test integration
  - [ ] CI/CD pipeline (GitHub Actions)
  - [ ] Code formatting setup (clang-format)

- [x] **Common Utilities**
  - [x] `types.hpp` - Basic type definitions
  - [x] `status.hpp` - Error handling
  - [x] `config.hpp` - Configuration constants
  - [x] `logger.hpp` - Logging wrapper

### 1.2 Disk Manager
- [x] `DiskManager` class
  - [x] File creation/opening
  - [x] Page read/write operations
  - [ ] Free page tracking
  - [x] Unit tests written (4 tests passing)

### 1.3 Page Management
- [x] `Page` class
  - [x] Page header structure (32 bytes, properly aligned)
  - [x] Slotted page format (`TablePage` class)
  - [x] Record insertion/deletion
  - [x] Record update
  - [x] Page compaction
  - [x] Page linking (next/prev for heap files)
  - [x] Unit tests written (27 tests passing)

### 1.4 Buffer Pool
- [x] `LRUReplacer` class
  - [x] LRU eviction policy
  - [x] Pin/unpin tracking
  - [x] Unit tests written (6 tests passing)

- [x] `BufferPoolManager` class
  - [x] Page fetching with caching
  - [x] Dirty page tracking
  - [x] Page eviction
  - [ ] Thread safety (basic)
  - [x] Unit tests written (5 tests passing)

### 1.5 B+ Tree Index
- [x] `BPlusTreePage` base class
  - [x] Internal node structure (`BPTreeInternalPage`)
  - [x] Leaf node structure (`BPTreeLeafPage`)
  - [x] Binary search for key lookup
  - [x] Insert/remove operations with shifting
  - [x] Sibling pointers for range scans
  - [x] Split support (move_half_to)
  - [x] Unit tests written (24 tests passing)

- [x] `BPlusTree` class
  - [x] Key insertion with splits
  - [x] Key deletion (simplified, no merging)
  - [x] Point lookup
  - [x] Range scan (iterator with lower_bound)
  - [x] Unit tests written (26 tests passing)
  - [ ] Concurrent access (stretch goal)
  - [ ] Full rebalancing on delete (future)

### 1.6 Table Storage
- [x] `Tuple` class
  - [x] Serialization/deserialization
  - [x] Null bitmap handling
  - [x] Variable-length columns (VARCHAR)
  - [x] All SQL types supported
  - [x] Unit tests written (46 tests passing)

- [x] `TupleValue` class
  - [x] Variant-based value storage
  - [x] Type checking and accessors
  - [x] Serialization to/from bytes

- [x] `TableHeap` class
  - [x] Tuple insertion
  - [x] Tuple update (in-place or relocate)
  - [x] Tuple deletion (mark)
  - [x] Sequential scan (TableIterator)
  - [x] Page allocation and linking
  - [x] Unit tests written (23 tests passing)

---

## Phase 2: Transaction & Concurrency

### 2.1 Transaction Manager
- [x] `Transaction` class (8 tests)
- [x] `TransactionManager` class (11 tests)

### 2.2 Write-Ahead Logging
- [x] `LogRecord` class (14 tests)
- [x] `WALManager` class (6 tests)

### 2.3 Lock Manager
- [x] `LockManager` class (26 tests)
  - [x] Shared/Exclusive locks
  - [x] Lock wait with timeout
  - [x] Deadlock detection (wait-for graph)

### 2.4 MVCC
- [x] `MVCCManager` class (11 tests)
  - [x] Snapshot isolation visibility
  - [x] Version lifecycle management

### 2.5 Recovery
- [x] `RecoveryManager` class (8 tests)
  - [x] ARIES-style recovery

---

## Phase 3: Query Processing

### 3.1 Catalog
- [x] `Column` class - data type definitions
- [x] `Schema` class - column collection
- [x] `Catalog` class with TableInfo
  - [x] TableInfo struct (oid, name, schema, TableHeap)
  - [x] Table creation with automatic TableHeap
  - [x] Table lookup and listing
  - [x] Unit tests (8 tests passing)

### 3.2 SQL Parser
- [x] Custom recursive descent parser (no external deps)
  - [x] Tokenizer/Lexer with 45+ token types
  - [x] Statement AST (SELECT, INSERT, UPDATE, DELETE, CREATE/DROP TABLE)
  - [x] Expression system (constants, columns, operators)
  - [x] 27 parser/lexer/expression tests passing

### 3.3 Binder
- [x] `Binder` class
  - [x] Name resolution (table → catalog, column → schema index)
  - [x] Type checking
  - [x] Bound contexts for each statement type
  - [x] 8 binder tests passing

### 3.4 Execution Engine
- [x] `Executor` base class with init()/next() iterator model
- [x] `SeqScanExecutor` - TableHeap iteration with optional predicate
- [x] `FilterExecutor` - predicate-based tuple filtering
- [x] `ProjectionExecutor` - column selection with output schema
- [x] `InsertExecutor` - batch tuple insertion
- [x] `UpdateExecutor` - SET clause with type conversion
- [x] `DeleteExecutor` - child-based deletion
- [x] 9 executor tests passing

### 3.5 Database API Integration
- [x] `Database.execute()` - full SQL pipeline
  - [x] Parser → Binder → Executors → Result
  - [x] Type conversion (int64→int32 for INTEGER columns)
  - [x] 16 end-to-end integration tests passing

### 3.6 Remaining (Query Processing)
- [x] JOINs (Nested Loop Join - INNER/LEFT/RIGHT/CROSS)
- [x] Aggregations (COUNT, SUM, AVG, MIN, MAX with GROUP BY)
- [x] ORDER BY / LIMIT
- [x] Hash Join (O(n+m) for equi-joins)
- [x] IndexScan executor (O(log n) point lookups, O(log n+k) range scans)

---

## Phase 4: Optimization & Polish

### 4.1 Query Optimizer
- [x] `PlanNode` classes (SeqScan, IndexScan, Filter, Sort, Limit, Join, Aggregation)
- [x] `CostModel` class (operator cost formulas)
- [x] `Statistics` class (cardinality/selectivity estimation)
- [x] `IndexSelector` class (auto SeqScan vs IndexScan)
- [x] Optimizer integration into Database.execute()
- [x] EXPLAIN command
- [x] Optimization tests (10 tests)

### 4.2 Hash Index
- [x] Extendible hashing (HashBucket, HashIndex)
- [x] Directory doubling
- [x] Bucket splitting
- [x] Hash index tests (7 tests)

### 4.3 Page Compression
- [x] LZ4 integration in CMake
- [x] PageCompression class
- [x] Compression header with checksum
- [x] Compile-time conditional support
- [x] Page compression tests (3 tests)

### 4.4 API & Shell
- [x] Interactive SQL shell improvements
  - [x] .help, .tables, .schema, .clear, .quit commands
  - [x] ASCII table formatting with column widths
  - [x] Banner display
- [x] EXPLAIN command help

---

## Test Summary

| Component | Test File | Tests Passing |
|-----------|-----------|---------------|
| Storage | 9 files | 161 |
| Transaction | 1 file | 84 |
| Catalog | 1 file | 8 |
| Parser/Binder | 1 file | 35 |
| Execution | 1 file | 28 |
| Integration | 1 file | 16 |
| **Total** | | **332** |

---

## Known Issues & Technical Debt

| Issue | Priority | Notes |
|-------|----------|-------|
| TableHeap update may change RID | Medium | When tuple grows, delete+insert occurs |
| No free space map | Low | Linear scan for page with space |
| No thread safety in TableHeap | Medium | Add later with lock manager integration |
| MVCC not integrated into execution | Medium | Executors don't check visibility |

---

*Last Updated: 2025-12-18*
