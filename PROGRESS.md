# Entropy Database - Development Progress

> **Purpose**: Track development progress across all phases. Update this document after completing any significant work. This is the single source of truth for project status.

---

## Quick Status

| Phase | Status | Progress |
|-------|--------|----------|
| Phase 1: Storage Engine | 🟢 Complete | ~90% |
| Phase 2: Transactions | 🟢 Complete | ~95% |
| Phase 3: Query Processing | 🔴 Not Started | 0% |
| Phase 4: Optimization | 🔴 Not Started | 0% |

**Overall Progress**: ~46%

**Last Updated**: 2024-12 - Recovery Manager & MVCC Complete (256 tests passing)

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
  - [x] All SQL types supported (BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR)
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
- [x] `Transaction` class
  - [x] Transaction ID generation
  - [x] Transaction state machine (2PL: GROWING → SHRINKING → COMMITTED/ABORTED)
  - [x] Write set tracking for undo
  - [x] Lock tracking for 2PL
  - [x] MVCC timestamps (start_ts, commit_ts)
  - [x] Unit tests passing (8 tests)

- [x] `TransactionManager` class
  - [x] Begin/Commit/Abort operations
  - [x] Active transaction tracking
  - [x] WAL integration (log insert/delete/update)
  - [x] Force-on-commit WAL flushing
  - [x] Unit tests passing (11 tests)

### 2.2 Write-Ahead Logging
- [x] `LogRecord` class
  - [x] 32-byte fixed header with serialization
  - [x] Multiple record types (BEGIN, COMMIT, ABORT, INSERT, DELETE, UPDATE, CHECKPOINT)
  - [x] Full serialize/deserialize support
  - [x] Unit tests passing (14 tests)

- [x] `WALManager` class
  - [x] Log appending with buffered writes
  - [x] Log flushing (fsync)
  - [x] Flush-to-LSN for commit
  - [x] Log recovery/reading
  - [ ] Group commit optimization
  - [x] Unit tests passing (6 tests)

### 2.3 Lock Manager
- [x] `LockManager` class
  - [x] Shared/Exclusive locks
  - [x] Lock table structure with request queues
  - [x] Lock wait with timeout
  - [x] Lock upgrade (S → X)
  - [x] Deadlock detection (wait-for graph)
  - [x] 2PL protocol enforcement
  - [x] Unit tests passing (26 tests)

### 2.4 MVCC
- [x] `MVCCManager` class
  - [x] VersionInfo structure (32 bytes: created_by, deleted_by, begin_ts, end_ts)
  - [x] Snapshot isolation visibility checking
  - [x] Read-committed isolation support
  - [x] Version lifecycle (init, delete, commit, rollback)
  - [x] Monotonic timestamp generation
  - [x] Unit tests passing (11 tests)

### 2.5 Recovery
- [x] `RecoveryManager` class (ARIES-style)
  - [x] Analysis phase (build ATT from checkpoint)
  - [x] REDO phase (replay logged operations)
  - [x] UNDO phase (rollback via prevLSN chains)
  - [x] Checkpoint creation
  - [x] Crash recovery tests (8 tests)

---

## Phase 3: Query Processing

### 3.1 Catalog
- [x] `Column` class
  - [x] Data type definitions
  - [ ] Constraints (NOT NULL, etc.)
  - [x] Unit tests passing

- [x] `Schema` class
  - [x] Column collection
  - [x] Column lookup by name
  - [x] Unit tests passing

- [ ] `Catalog` class
  - [ ] Table metadata storage
  - [ ] Index metadata storage
  - [ ] System tables
  - [ ] Unit tests passing

### 3.2 SQL Parser
- [ ] Parser integration
  - [ ] hsql library integration
  - [ ] AST wrapper classes
  - [ ] Unit tests passing

- [ ] `Binder` class
  - [ ] Name resolution
  - [ ] Type checking
  - [ ] Unit tests passing

### 3.3 Execution Engine
- [ ] `Executor` base class
  - [ ] Iterator model (Next())
  - [ ] Context management

- [ ] Scan Executors
  - [ ] `SeqScanExecutor`
  - [ ] `IndexScanExecutor`
  - [ ] Unit tests passing

- [ ] Modification Executors
  - [ ] `InsertExecutor`
  - [ ] `UpdateExecutor`
  - [ ] `DeleteExecutor`
  - [ ] Unit tests passing

- [ ] Join Executors
  - [ ] `NestedLoopJoinExecutor`
  - [ ] `HashJoinExecutor`
  - [ ] Unit tests passing

- [ ] Other Executors
  - [ ] `AggregationExecutor` (COUNT, SUM, AVG, MIN, MAX)
  - [ ] `ProjectionExecutor`
  - [ ] `FilterExecutor`
  - [ ] `SortExecutor`
  - [ ] `LimitExecutor`
  - [ ] Unit tests passing

---

## Phase 4: Optimization & Polish

### 4.1 Query Optimizer
- [ ] `PlanNode` classes
  - [ ] Logical plan nodes
  - [ ] Physical plan nodes
  - [ ] Plan tree structure

- [ ] `CostModel` class
  - [ ] I/O cost estimation
  - [ ] CPU cost estimation
  - [ ] Selectivity estimation

- [ ] `Statistics` class
  - [ ] Table cardinality
  - [ ] Column histograms
  - [ ] Index statistics

- [ ] `Optimizer` class
  - [ ] Rule-based optimizations
  - [ ] Index selection
  - [ ] Join ordering
  - [ ] Predicate pushdown

### 4.2 Hash Index
- [ ] `HashIndex` class
  - [ ] Extendible hashing
  - [ ] Point lookups
  - [ ] Unit tests passing

### 4.3 Advanced Features
- [ ] EXPLAIN command
- [ ] Page compression (LZ4)
- [ ] Bulk loading optimization
- [ ] Covering index queries

### 4.4 API & Shell
- [ ] `Database` class (public API)
- [ ] `Result` class
- [ ] Interactive SQL shell
- [ ] Error messages

### 4.5 Documentation & Polish
- [ ] README with architecture diagram
- [ ] API documentation
- [ ] Example queries
- [ ] Performance benchmarks

---

## Benchmarks & Testing

### Unit Test Coverage
| Component | Tests Written | Tests Passing | Coverage |
|-----------|---------------|---------------|----------|
| Storage | 9 files | 161 passing | ~80% |
| Catalog | 1 file | 5 passing | ~10% |
| Transaction | 1 file | 84 passing | ~90% |
| Execution | 0 | 0 | 0% |
| Optimizer | 0 | 0 | 0% |
| Integration | 1 file | Passing | ~5% |

### Performance Benchmarks
| Benchmark | Target | Current | Status |
|-----------|--------|---------|--------|
| Bulk Insert | 50K/sec | - | Not Run |
| Point Query | <1ms | - | Not Run |
| Range Query | 100K/sec | - | Not Run |
| TPS | 10K | - | Not Run |

### Integration Tests
- [ ] End-to-end SQL tests
- [ ] Crash recovery test
- [ ] Concurrent access test
- [ ] Large dataset test

---

## Development Log

### Session: 2024-12 - Recovery Manager & MVCC Implementation
- Implemented ARIES-style Recovery Manager (`recovery.hpp`, `recovery.cpp`):
  - Analysis phase: scans log from checkpoint, builds Active Transaction Table (ATT)
  - REDO phase: replays all logged data modification operations
  - UNDO phase: rolls back uncommitted transactions via prevLSN chains
  - Checkpoint creation support for faster recovery
- Implemented MVCC (`mvcc.hpp`, `mvcc.cpp`):
  - VersionInfo: 32-byte structure with created_by, deleted_by, begin_ts, end_ts
  - Snapshot isolation visibility: transaction sees committed versions from before its start
  - Read-committed isolation support
  - Version lifecycle management (init, mark_deleted, finalize_commit, rollback)
  - Monotonic timestamp generation for transaction ordering
- Added 19 new tests (8 Recovery + 11 MVCC)
- All 256 tests passing (161 storage + 84 transaction + 5 catalog + 6 integration)

### Session: 2024-12 - Lock Manager Implementation
- Implemented complete Lock Manager for 2PL concurrency control:
  - Shared (S) and Exclusive (X) lock modes with compatibility matrix
  - Lock request queues for each resource (table or row)
  - Lock wait with configurable timeout
  - Lock upgrade (shared → exclusive) with proper handling
  - Deadlock detection using wait-for graph with DFS cycle detection
  - 2PL protocol enforcement (GROWING → SHRINKING phase transition)
  - Thread-safe implementation with condition variables
  - Release all locks helper for transaction cleanup
- Added 26 comprehensive Lock Manager tests
- All 231 tests passing

### Session: 2024-12 - B+ Tree Core Operations
- Implemented complete B+ Tree index with all core operations
  - Insert with automatic leaf and internal node splitting
  - Point lookup (find) with O(log n) tree traversal
  - Range scan using BPlusTreeIterator with lower_bound support
  - Simplified delete (marks removed, no rebalancing)
  - is_empty() checks actual key count in root
- Added 26 comprehensive B+ Tree tests covering:
  - Basic operations (insert, find, remove)
  - Duplicate handling
  - Range scans with various boundaries
  - Iterator traversal and lower_bound
  - Large insertions triggering splits
  - Random and reverse order insertions
  - Delete-all and reinsert scenarios
- All 161 storage tests passing

### Session: 2024-12 - Storage Engine Core Implementation
- Implemented complete slotted page format (`TablePage` class)
  - Record insertion, deletion, update
  - Page compaction for space reclamation
  - Page linking for heap files
  - 19 comprehensive tests
- Implemented full Tuple serialization
  - `TupleValue` class with variant-based storage for all SQL types
  - Null bitmap handling
  - Fixed-size and variable-length column support
  - Schema-based serialization/deserialization
  - 46 comprehensive tests (TupleValue + Tuple)
- Implemented TableHeap with iterator support
  - CRUD operations (insert, update, delete, get)
  - Sequential scan via TableIterator
  - Multi-page support with automatic page allocation
  - 23 comprehensive tests
- Fixed PageHeader alignment issue (40 bytes -> 32 bytes)
- All 111 storage tests passing

### Session: 2024 - Project Initialization
- Created comprehensive project structure with all directories
- Set up CMake build system with FetchContent for dependencies
- Created DESIGN.md with architecture documentation
- Created PROGRESS.md for tracking
- Created base README.md for portfolio
- Implemented core utilities: types, status, config, logger
- Implemented basic storage layer: Page, DiskManager, BufferPool, LRUReplacer
- Created stub implementations for all planned components
- Set up Google Test with initial test files
- Created example program and SQL shell skeleton

---

## Known Issues & Technical Debt

| Issue | Priority | Notes |
|-------|----------|-------|
| TableHeap update may change RID | Medium | When tuple grows, delete+insert occurs |
| No free space map | Low | Linear scan for page with space |
| No thread safety in TableHeap | Medium | Add later with lock manager |

---

## Next Steps

1. **Immediate**: Begin catalog system for table metadata
2. **Next**: Start query processing (parser, binder)
3. **Then**: Implement execution engine
4. **Following**: Query optimization

---

*Update this document after each development session*
