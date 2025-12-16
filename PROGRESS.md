# Entropy Database - Development Progress

> **Purpose**: Track development progress across all phases. Update this document after completing any significant work. This is the single source of truth for project status.

---

## Quick Status

| Phase | Status | Progress |
|-------|--------|----------|
| Phase 1: Storage Engine | 🟡 In Progress | ~55% |
| Phase 2: Transactions | 🔴 Not Started | 0% |
| Phase 3: Query Processing | 🔴 Not Started | 0% |
| Phase 4: Optimization | 🔴 Not Started | 0% |

**Overall Progress**: ~18%

**Last Updated**: 2024-12 - B+ Tree Page Structures Complete (135 tests passing)

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

- [ ] `BPlusTree` class
  - [ ] Key insertion with splits
  - [ ] Key deletion with merging
  - [ ] Point lookup
  - [ ] Range scan (iterator)
  - [ ] Unit tests passing
  - [ ] Concurrent access (stretch goal)

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
- [ ] `Transaction` class
  - [ ] Transaction ID generation
  - [ ] Transaction state machine
  - [ ] Write set tracking
  - [ ] Unit tests passing

- [ ] `TransactionManager` class
  - [ ] Begin/Commit/Abort
  - [ ] Active transaction tracking
  - [ ] Unit tests passing

### 2.2 Write-Ahead Logging
- [ ] `LogRecord` class
  - [ ] Record serialization
  - [ ] Multiple record types (BEGIN, COMMIT, INSERT, etc.)
  - [ ] Unit tests passing

- [ ] `WALManager` class
  - [ ] Log appending
  - [ ] Log flushing (fsync)
  - [ ] Group commit optimization
  - [ ] Unit tests passing

### 2.3 Lock Manager
- [ ] `LockManager` class
  - [ ] Shared/Exclusive locks
  - [ ] Lock table structure
  - [ ] Deadlock detection (basic)
  - [ ] Unit tests passing

### 2.4 MVCC
- [ ] `VersionChain` structure
  - [ ] Version storage
  - [ ] Visibility checking
  - [ ] Unit tests passing

- [ ] MVCC integration
  - [ ] Snapshot isolation
  - [ ] Read uncommitted/committed modes
  - [ ] Unit tests passing

### 2.5 Recovery
- [ ] `RecoveryManager` class
  - [ ] WAL replay (REDO)
  - [ ] Transaction rollback (UNDO)
  - [ ] Checkpoint creation
  - [ ] Crash recovery test

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
| Storage | 8 files | 135 passing | ~65% |
| Catalog | 1 file | Passing | ~10% |
| Transaction | 0 | 0 | 0% |
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

1. **Immediate**: Implement B+ Tree index page structures
2. **Next**: Implement B+ Tree core operations (insert, search)
3. **Then**: Implement B+ Tree range scan and deletion
4. **Following**: Begin transaction management

---

*Update this document after each development session*
