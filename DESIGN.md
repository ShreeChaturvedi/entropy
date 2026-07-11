# Entropy Database - Design & Architecture Document

> **Purpose**: This document serves as the single source of truth for all design decisions, architecture choices, implemented components, and technical specifications. It must be kept up-to-date as the project evolves to prevent inconsistencies and help future development.

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Technology Stack](#technology-stack)
3. [Directory Structure](#directory-structure)
4. [Component Architecture](#component-architecture)
5. [Implemented Components](#implemented-components)
6. [Data Structures & Formats](#data-structures--formats)
7. [Threading Model](#threading-model)
8. [External Dependencies](#external-dependencies)
9. [Coding Conventions](#coding-conventions)
10. [Key Design Decisions](#key-design-decisions)

---

## Project Overview

**Entropy** is a high-performance relational database management system (RDBMS) built from scratch in modern C++20. It demonstrates systems programming expertise through:
- Custom slotted-page storage with a latch-crabbing B+ tree and CRC-32 page checksums
- MVCC-based transaction management (snapshot isolation, first-updater-wins)
- Cost-based query optimization
- ACID compliance with WAL and ARIES-style crash recovery
- A deterministic, FoundationDB-style crash simulator with seeded fault injection

**Scope Boundaries** (explicitly NOT implementing):
- Distributed features / replication
- Network protocol (local-only)
- Window functions, CTEs, complex subqueries
- Stored procedures / triggers

---

## Technology Stack

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Language | C++20 | Modern features (concepts, ranges, coroutines available) |
| Build System | CMake 3.20+ | Cross-platform, industry standard |
| Testing | Google Test | Well-supported, familiar to reviewers |
| Benchmarking | Google Benchmark | Pairs with GTest, micro-benchmark support |
| SQL Parser | **Custom** | Hand-written recursive descent parser; no external dependencies |
| Logging | spdlog | Header-only, fast, modern |

---

## Directory Structure

```
entropy/
├── CMakeLists.txt              # Root CMake configuration
├── README.md                   # Project overview for GitHub
├── DESIGN.md                   # This file - architecture decisions
├── LICENSE                     # MIT License
│
├── cmake/                      # CMake modules and config
│   ├── CompilerWarnings.cmake  # Warning flags configuration
│   ├── Dependencies.cmake      # External dependency fetching
│   └── Testing.cmake           # Test configuration
│
├── include/                    # Public headers (API surface)
│   └── entropy/
│       ├── entropy.hpp         # Main include header
│       ├── database.hpp        # Database class interface
│       ├── result.hpp          # Query result types
│       └── status.hpp          # Error/status codes
│
├── src/                        # Implementation source files
│   ├── CMakeLists.txt
│   │
│   ├── common/                 # Shared utilities
│   │   ├── config.hpp          # Configuration constants
│   │   ├── config.cpp
│   │   ├── types.hpp           # Common type definitions
│   │   ├── status.hpp          # Internal status codes
│   │   ├── status.cpp
│   │   ├── logger.hpp          # Logging wrapper
│   │   ├── logger.cpp
│   │   └── macros.hpp          # Utility macros
│   │
│   ├── storage/                # Storage engine layer
│   │   ├── CMakeLists.txt
│   │   ├── page.hpp            # Page structure definition
│   │   ├── page.cpp
│   │   ├── page_id.hpp         # Page identifier type
│   │   ├── disk_manager.hpp    # Disk I/O operations
│   │   ├── disk_manager.cpp
│   │   ├── buffer_pool.hpp     # Buffer pool manager
│   │   ├── buffer_pool.cpp
│   │   ├── lru_replacer.hpp    # LRU eviction policy
│   │   ├── lru_replacer.cpp
│   │   ├── b_plus_tree.hpp     # B+ tree index structure
│   │   ├── b_plus_tree.cpp
│   │   ├── b_plus_tree_page.hpp      # B+ tree node pages
│   │   ├── b_plus_tree_leaf.hpp      # Leaf node specifics
│   │   ├── b_plus_tree_internal.hpp  # Internal node specifics
│   │   ├── hash_index.hpp      # Hash index implementation
│   │   ├── hash_index.cpp
│   │   ├── table_heap.hpp      # Table storage (heap file)
│   │   ├── table_heap.cpp
│   │   ├── table_page.hpp      # Slotted page for heap records
│   │   ├── table_page.cpp
│   │   ├── tuple.hpp           # Row/tuple representation
│   │   └── tuple.cpp
│   │
│   ├── catalog/                # Schema/metadata management
│   │   ├── CMakeLists.txt
│   │   ├── catalog.hpp         # System catalog
│   │   ├── catalog.cpp
│   │   ├── catalog_manifest.hpp    # Durable, fsync'd catalog manifest
│   │   ├── catalog_manifest.cpp
│   │   ├── schema.hpp          # Table schema definition
│   │   ├── schema.cpp
│   │   ├── column.hpp          # Column metadata
│   │   └── column.cpp
│   │
│   ├── transaction/            # Transaction management
│   │   ├── CMakeLists.txt
│   │   ├── transaction.hpp     # Transaction object
│   │   ├── transaction.cpp
│   │   ├── transaction_manager.hpp   # Transaction lifecycle
│   │   ├── transaction_manager.cpp
│   │   ├── lock_manager.hpp    # Row-level lock manager, deadlock detection
│   │   ├── lock_manager.cpp
│   │   ├── mvcc.hpp            # MVCC visibility rules
│   │   ├── mvcc.cpp
│   │   ├── version_store.hpp   # Per-RID version chains, conflict detection
│   │   ├── version_store.cpp
│   │   ├── wal.hpp             # Write-ahead log
│   │   ├── wal.cpp
│   │   ├── log_record.hpp      # WAL record format
│   │   ├── log_record.cpp
│   │   ├── recovery.hpp        # Crash recovery (ARIES-style)
│   │   └── recovery.cpp
│   │
│   ├── execution/              # Query execution engine
│   │   ├── CMakeLists.txt
│   │   ├── executor.hpp        # Executor interface
│   │   ├── executor.cpp
│   │   ├── executor_context.hpp      # Execution context
│   │   ├── seq_scan_executor.hpp     # Sequential scan
│   │   ├── seq_scan_executor.cpp
│   │   ├── index_scan_executor.hpp   # Index scan
│   │   ├── index_scan_executor.cpp
│   │   ├── insert_executor.hpp       # INSERT execution
│   │   ├── insert_executor.cpp
│   │   ├── update_executor.hpp       # UPDATE execution
│   │   ├── update_executor.cpp
│   │   ├── delete_executor.hpp       # DELETE execution
│   │   ├── delete_executor.cpp
│   │   ├── nested_loop_join.hpp      # Nested loop join
│   │   ├── nested_loop_join.cpp
│   │   ├── hash_join.hpp             # Hash join
│   │   ├── hash_join.cpp
│   │   ├── aggregation.hpp           # Aggregation ops
│   │   ├── aggregation.cpp
│   │   ├── projection.hpp            # Projection
│   │   ├── projection.cpp
│   │   ├── filter.hpp                # Filter/WHERE
│   │   └── filter.cpp
│   │
│   ├── optimizer/              # Query optimizer
│   │   ├── CMakeLists.txt
│   │   ├── optimizer.hpp       # Optimizer interface
│   │   ├── optimizer.cpp
│   │   ├── plan_node.hpp       # Query plan nodes
│   │   ├── plan_node.cpp
│   │   ├── cost_model.hpp      # Cost estimation
│   │   ├── cost_model.cpp
│   │   ├── statistics.hpp      # Table/column statistics
│   │   ├── statistics.cpp
│   │   ├── index_selector.hpp  # Index selection
│   │   └── index_selector.cpp
│   │
│   ├── parser/                 # SQL parsing layer
│   │   ├── CMakeLists.txt
│   │   ├── parser.hpp          # Parser wrapper
│   │   ├── parser.cpp
│   │   ├── ast.hpp             # Abstract syntax tree
│   │   ├── ast.cpp
│   │   ├── binder.hpp          # Name resolution
│   │   └── binder.cpp
│   │
│   ├── api/                    # Public API implementation
│   │   ├── CMakeLists.txt
│   │   ├── database.cpp        # Database class impl
│   │   ├── result.cpp          # Result set handling
│   │   ├── shell.cpp           # Interactive SQL shell
│   │   └── shell_utils.cpp     # Shell statement splitting
│   │
│   └── sim/                    # Deterministic crash simulator
│       ├── fault.hpp           # Fault kinds, seeded PRNG streams
│       ├── schedule.hpp        # Replayable crash schedules
│       ├── schedule.cpp
│       ├── sim_disk_manager.hpp    # Fault-injecting page device
│       ├── sim_disk_manager.cpp
│       ├── sim_log_store.hpp   # Fault-injecting WAL store
│       ├── sim_log_store.cpp
│       ├── workload.hpp        # Workload + oracle + invariant checks
│       └── workload.cpp
│
├── tests/                      # Test files
│   ├── CMakeLists.txt
│   ├── common/                 # Common test utilities
│   │   └── test_utils.hpp
│   ├── storage/
│   │   ├── page_test.cpp
│   │   ├── disk_manager_test.cpp
│   │   ├── buffer_pool_test.cpp
│   │   ├── b_plus_tree_test.cpp
│   │   └── table_heap_test.cpp
│   ├── transaction/
│   │   ├── transaction_test.cpp
│   │   ├── wal_test.cpp
│   │   └── mvcc_test.cpp
│   ├── execution/
│   │   ├── executor_test.cpp
│   │   └── join_test.cpp
│   ├── optimizer/
│   │   └── optimizer_test.cpp
│   └── integration/
│       ├── sql_test.cpp        # End-to-end SQL tests
│       └── recovery_test.cpp   # Crash recovery tests
│
├── benchmarks/                 # Performance benchmarks
│   ├── CMakeLists.txt
│   ├── insert_benchmark.cpp
│   ├── query_benchmark.cpp
│   ├── transaction_benchmark.cpp
│   └── comparison/             # SQLite comparison
│       └── sqlite_comparison.cpp
│
├── examples/                   # Example usage
│   ├── CMakeLists.txt
│   ├── basic_usage.cpp
│   └── transaction_example.cpp
│
├── tools/                      # Development tools
│   ├── CMakeLists.txt
│   └── entropy-sim/            # Crash-simulation CLI runner
│       └── main.cpp
│
├── packaging/                  # Downstream packaging checks
│   └── consumer-smoke/         # Standalone find_package(entropy) consumer
│
└── third_party/                # External dependencies (gitignore'd or submodules)
    └── README.md               # Instructions for dependencies
```

---

## Component Architecture

### Layer Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        SQL Shell / API                          │
├─────────────────────────────────────────────────────────────────┤
│                     Parser (SQL → AST)                          │
├─────────────────────────────────────────────────────────────────┤
│                   Binder (Name Resolution)                      │
├─────────────────────────────────────────────────────────────────┤
│              Optimizer (AST → Execution Plan)                   │
│         ┌─────────────────┬─────────────────┐                   │
│         │   Cost Model    │ Index Selector  │                   │
│         └─────────────────┴─────────────────┘                   │
├─────────────────────────────────────────────────────────────────┤
│                  Execution Engine                               │
│    ┌──────────┬──────────┬──────────┬──────────┬──────────┐    │
│    │SeqScan   │IndexScan │ Insert   │ Update   │ Delete   │    │
│    ├──────────┼──────────┼──────────┼──────────┼──────────┤    │
│    │HashJoin  │NLJoin    │Aggregate │Project   │ Filter   │    │
│    └──────────┴──────────┴──────────┴──────────┴──────────┘    │
├─────────────────────────────────────────────────────────────────┤
│               Transaction Manager (MVCC)                        │
│         ┌─────────────────┬─────────────────┐                   │
│         │  Lock Manager   │      WAL        │                   │
│         └─────────────────┴─────────────────┘                   │
├─────────────────────────────────────────────────────────────────┤
│                    Catalog (Schema)                             │
├─────────────────────────────────────────────────────────────────┤
│                   Storage Engine                                │
│    ┌──────────┬──────────┬──────────┬──────────┐               │
│    │B+ Tree   │Hash Index│Table Heap│  Tuple   │               │
│    └──────────┴──────────┴──────────┴──────────┘               │
├─────────────────────────────────────────────────────────────────┤
│                  Buffer Pool Manager                            │
│         ┌─────────────────┬─────────────────┐                   │
│         │  LRU Replacer   │   Page Cache    │                   │
│         └─────────────────┴─────────────────┘                   │
├─────────────────────────────────────────────────────────────────┤
│                   Disk Manager                                  │
│         ┌─────────────────┬─────────────────┐                   │
│         │   Page I/O      │  File Handles   │                   │
│         └─────────────────┴─────────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Implemented Components

> **Status Legend**: ✅ Complete | 🟡 Partial | ⭕ Not Started

### Storage Layer
| Component | Status | Header | Implementation | Tests |
|-----------|--------|--------|----------------|-------|
| Page | ✅ Complete | `page.hpp` | `page.cpp` | 27 passing |
| DiskManager | ✅ Complete | `disk_manager.hpp` | `disk_manager.cpp` | 4 passing |
| BufferPool | ✅ Complete | `buffer_pool.hpp` | `buffer_pool.cpp` | 5 passing |
| LRUReplacer | ✅ Complete | `lru_replacer.hpp` | `lru_replacer.cpp` | 6 passing |
| B+Tree | ✅ Complete | `b_plus_tree.hpp` | `b_plus_tree.cpp` | 50 passing |
| HashIndex | ✅ Complete | `hash_index.hpp` | header-only | `hash_index_test.cpp` (in-memory extendible hash) |
| TableHeap | ✅ Complete | `table_heap.hpp` | `table_heap.cpp` | 23 passing |
| TablePage | ✅ Complete | `table_page.hpp` | `table_page.cpp` | `table_page_test.cpp` |
| Tuple | ✅ Complete | `tuple.hpp` | `tuple.cpp` | 46 passing |

### Catalog Layer
| Component | Status | Header | Implementation | Tests |
|-----------|--------|--------|----------------|-------|
| Catalog | ✅ Complete | `catalog.hpp` | `catalog.cpp` | 8 passing |
| CatalogManifest | ✅ Complete | `catalog_manifest.hpp` | `catalog_manifest.cpp` | `catalog_persistence_test.cpp` |
| Schema | ✅ Complete | `schema.hpp` | `schema.cpp` | included |
| Column | ✅ Complete | `column.hpp` | `column.cpp` | included |

### Transaction Layer
| Component | Status | Header | Implementation | Tests |
|-----------|--------|--------|----------------|-------|
| Transaction | ✅ Complete | `transaction.hpp` | `transaction.cpp` | 8 passing |
| TransactionManager | ✅ Complete | `transaction_manager.hpp` | `transaction_manager.cpp` | 11 passing |
| LockManager | ✅ Complete | `lock_manager.hpp` | `lock_manager.cpp` | 26 passing |
| MVCC | ✅ Complete | `mvcc.hpp` | `mvcc.cpp` | 11 passing |
| VersionStore | ✅ Complete | `version_store.hpp` | `version_store.cpp` | `version_store_test.cpp` |
| WAL | ✅ Complete | `wal.hpp` | `wal.cpp` | 6 passing |
| LogRecord | ✅ Complete | `log_record.hpp` | `log_record.cpp` | 14 passing |
| Recovery | ✅ Complete | `recovery.hpp` | `recovery.cpp` | 8 passing |

### Parser Layer
| Component | Status | Header | Implementation | Tests |
|-----------|--------|--------|----------------|-------|
| Lexer/Tokenizer | ✅ Complete | `token.hpp` | `token.cpp` | 8 passing |
| Parser | ✅ Complete | `parser.hpp` | `parser.cpp` | 13 passing |
| Statement AST | ✅ Complete | `statement.hpp` | - | - |
| Expression | ✅ Complete | `expression.hpp` | `expression.cpp` | 6 passing |
| Binder | ✅ Complete | `binder.hpp` | `binder.cpp` | 8 passing |

### Execution Layer
| Component | Status | Header | Implementation | Tests |
|-----------|--------|--------|----------------|-------|
| Executor Base | ✅ Complete | `executor.hpp` | `executor.cpp` | - |
| SeqScanExecutor | ✅ Complete | `seq_scan_executor.hpp` | `seq_scan_executor.cpp` | 3 passing |
| InsertExecutor | ✅ Complete | `insert_executor.hpp` | `insert_executor.cpp` | 2 passing |
| UpdateExecutor | ✅ Complete | `update_executor.hpp` | `update_executor.cpp` | 1 passing |
| DeleteExecutor | ✅ Complete | `delete_executor.hpp` | `delete_executor.cpp` | 1 passing |
| FilterExecutor | ✅ Complete | `filter.hpp` | `filter.cpp` | 1 passing |
| ProjectionExecutor | ✅ Complete | `projection.hpp` | `projection.cpp` | 1 passing |
| NestedLoopJoin | ✅ Complete | `nested_loop_join.hpp` | `nested_loop_join.cpp` | 3 passing |
| HashJoin | ✅ Complete | `hash_join.hpp` | `hash_join.cpp` | 2 passing |
| Aggregation | ✅ Complete | `aggregation.hpp` | `aggregation.cpp` | 5 passing |
| SortExecutor | ✅ Complete | `sort_executor.hpp` | `sort_executor.cpp` | 2 passing |
| LimitExecutor | ✅ Complete | `limit_executor.hpp` | `limit_executor.cpp` | 3 passing |
| IndexScanExecutor | ✅ Complete | `index_scan_executor.hpp` | `index_scan_executor.cpp` | 4 passing |

### Optimizer Layer
| Component | Status | Header | Implementation | Tests |
|-----------|--------|--------|----------------|-------|
| Optimizer | ✅ Complete | `optimizer.hpp` | `optimizer.cpp` | `optimizer_test.cpp` |
| CostModel | ✅ Complete | `cost_model.hpp` | `cost_model.cpp` | - |
| Statistics | ✅ Complete | `statistics.hpp` | `statistics.cpp` | - |
| IndexSelector | ✅ Complete | `index_selector.hpp` | `index_selector.cpp` | - |
| PlanNode | ✅ Complete | `plan_node.hpp` | - | - |

### API Layer
| Component | Status | Header | Implementation | Tests |
|-----------|--------|--------|----------------|-------|
| Database | ✅ Complete | `database.hpp` | `database.cpp` | 16 passing |
| Result | ✅ Complete | `result.hpp` | `result.cpp` | included |
| Shell | ✅ Complete | `shell.hpp` | `shell.cpp` | interactive shell with `.help`/`.tables`/`.schema` meta-commands |

### Simulation Layer
| Component | Status | Header | Implementation | Tests |
|-----------|--------|--------|----------------|-------|
| SimDiskManager | ✅ Complete | `sim_disk_manager.hpp` | `sim_disk_manager.cpp` | `sim_test.cpp` |
| SimLogStore | ✅ Complete | `sim_log_store.hpp` | `sim_log_store.cpp` | `sim_test.cpp` |
| Schedule/Fault | ✅ Complete | `schedule.hpp`, `fault.hpp` | `schedule.cpp` | `sim_test.cpp` |
| Workload/Oracle | ✅ Complete | `workload.hpp` | `workload.cpp` | `sim_test.cpp` |
| entropy-sim CLI | ✅ Complete | - | `tools/entropy-sim/main.cpp` | run in CI |

---

## Data Structures & Formats

### Page Format (4KB default)
```
┌──────────────────────────────────────────────────────────────┐
│ Page Header (32 bytes)                                       │
│ ┌──────────────┬──────────────┬──────────────┬─────────────┐│
│ │ page_id (4B) │ page_type(1B)│ record_cnt(2)│ free_ptr(2B)││
│ │ lsn (8B)     │ checksum (4B)│ reserved (11B)             ││
│ └──────────────┴──────────────┴──────────────┴─────────────┘│
├──────────────────────────────────────────────────────────────┤
│ Slot Array (grows downward from header)                      │
│ [slot_0][slot_1][slot_2]...                                  │
│ Each slot: offset(2B) + length(2B) = 4 bytes                 │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│                    Free Space                                │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│ Record Data (grows upward from bottom)                       │
│ ...record_2|record_1|record_0]                               │
└──────────────────────────────────────────────────────────────┘
```

### B+ Tree Node Format
```
Internal Node:
┌─────────────────────────────────────────────────────────────┐
│ [Header] | key_0 | ptr_0 | key_1 | ptr_1 | ... | ptr_n     │
└─────────────────────────────────────────────────────────────┘

Leaf Node:
┌─────────────────────────────────────────────────────────────┐
│ [Header] | key_0 | rid_0 | key_1 | rid_1 | ... | next_leaf │
└─────────────────────────────────────────────────────────────┘
```

### WAL Record Format
```
┌──────────────────────────────────────────────────────────────┐
│ LSN (8B) │ TxnID (4B) │ PrevLSN (8B) │ Type (1B) │ Size (4B)│
├──────────────────────────────────────────────────────────────┤
│                    Payload (variable)                        │
├──────────────────────────────────────────────────────────────┤
│                    Checksum (4B)                             │
└──────────────────────────────────────────────────────────────┘
```

### Tuple Format
```
┌──────────────────────────────────────────────────────────────┐
│ Null Bitmap │ Col_0 │ Col_1 │ Col_2 │ ... │ Variable Data   │
└──────────────────────────────────────────────────────────────┘
```

---

## Threading Model

### Concurrency Strategy
1. **Buffer Pool**: Thread-safe (internal mutex); every `Page` carries a
   shared/exclusive latch used by the B+ tree
2. **B+ Tree**: Latch crabbing (hand-over-hand page latches) — concurrent
   readers and range scans; structural writers (insert/delete) are serialized
   and shed ancestor latches once a node is split/merge-safe
3. **Transaction Manager**: Centralized lock manager with row-level locking + MVCC
4. **WAL**: Single-writer, fsync'd and forced to the commit LSN on commit

### Lock Hierarchy (to prevent deadlocks)
1. Lock Manager mutex
2. Transaction mutexes
3. B+ tree writer mutex (serializes structural writers)
4. Page latches (acquired via crabbing: ancestors before descendants,
   left siblings before right)
5. Buffer pool internal mutex (leaf-level; never held while waiting on a
   page latch)

B+ tree deadlock freedom is reader-vs-writer (writer serialization removes
writer-vs-writer ordering; readers run concurrently with the writer) and has
two parts. At the leaf level all latch waits go left-to-right: a writer
turning left first releases any right-sibling latch before re-latching the
node, and parent-pointer maintenance is latch-free (the field is writer-only;
latching would touch pages left of held leaf latches). At internal levels the
writer's re-latches do not follow a global order but cannot block: the
underflowing node's parent stays write-latched, fencing descent readers out of
the subtree, and chain scanners never hold internal pages. A grown root is
published unlatched via a release-store of the root id (readers use an acquire
load). See the design notes in b_plus_tree.cpp for the full argument.

---

## External Dependencies

| Library | Version | Purpose | Integration |
|---------|---------|---------|-------------|
| Google Test | 1.14+ | Unit testing | FetchContent |
| Google Benchmark | 1.8+ | Performance benchmarks | FetchContent |
| spdlog | 1.12+ | Logging | FetchContent |
| SQLite3 | system | Optional benchmark comparison baseline | `find_package` (opt-in) |

The SQL parser is hand-written (see the Technology Stack table). Entropy takes
no third-party parsing dependency.

---

## Coding Conventions

### Naming
- **Classes/Structs**: PascalCase (`BufferPoolManager`)
- **Functions/Methods**: snake_case (`get_page()`)
- **Variables**: snake_case (`page_count`)
- **Constants**: kPascalCase (`kPageSize`)
- **Macros**: SCREAMING_SNAKE_CASE (`ENTROPY_ASSERT`)
- **Files**: snake_case (`buffer_pool.hpp`)

### Code Style
- 100 character line limit
- 4 spaces for indentation (no tabs)
- `#pragma once` for header guards
- Always use braces for control structures
- Prefer `auto` for iterators and complex types
- Use `const` and `constexpr` liberally
- Use `[[nodiscard]]` for functions with important return values

### Memory Management
- **NO raw `new`/`delete`** - use smart pointers
- `std::unique_ptr` for single ownership
- `std::shared_ptr` only when truly needed
- RAII for all resource management (files, locks, etc.)

### Error Handling
- Use `Status` return type for operations that can fail
- Exceptions only for truly exceptional cases
- Never throw in destructors
- Use `ENTROPY_ASSERT` for invariants in debug builds

---

## Key Design Decisions

### Decision Log

| Date | Decision | Rationale | Alternatives Considered |
|------|----------|-----------|------------------------|
| 2024-12 | Use slotted page format | Industry standard, supports variable-length records | Fixed-length only |
| 2024-12 | MVCC over strict 2PL | Better read performance, snapshot isolation | Strict 2PL only |
| 2024-12 | Custom SQL parser | No external dependencies, full control over grammar | hsql, libpg_query |
| 2024-12 | Volcano iterator model | Industry standard, composable executors | Vectorized, push-based |
| 2024-12 | TableInfo with embedded TableHeap | Catalog directly manages storage, simpler integration | Separate TableHeap registry |
| 2024-12 | Schema-aware type conversion | int64→int32 at insertion time, not evaluation time | Type coercion in expressions |

### Architecture Decisions Records (ADR)

#### ADR-001: Page Size
- **Context**: Need to choose default page size
- **Decision**: 4KB default, configurable
- **Consequences**: Matches OS page size, good for SSD, may need larger for wide tables

#### ADR-002: Index Structure
- **Context**: Primary index implementation choice
- **Decision**: B+ tree as primary, extendible hash index for equality
- **Consequences**: B+ tree handles range queries, hash for O(1) point lookups

#### ADR-003: Concurrency Control
- **Context**: How to handle concurrent transactions
- **Decision**: MVCC snapshot isolation + lock manager (wait-for-graph deadlock detection, wait-die victim selection)
- **Consequences**: Readers don't block writers. Write-write conflicts resolved first-updater-wins. Old versions need garbage collection.

#### ADR-004: SQL Parser Design
- **Context**: Whether to use external SQL parser library
- **Decision**: Custom recursive descent parser with no external dependencies
- **Rationale**:
  - Full control over supported SQL subset
  - No version compatibility issues with third-party libraries
  - Educational value for understanding parser internals
  - Simpler build and deployment
- **Trade-offs**: More code to maintain, may have grammar edge cases

#### ADR-005: Execution Engine Architecture
- **Context**: How to execute query plans
- **Decision**: Volcano iterator model with `init()` / `next()` interface
- **Rationale**:
  - Industry standard pattern (used in PostgreSQL, MySQL)
  - Easy to compose executors into pipelines
  - Natural for memory-limited processing (one tuple at a time)
- **Trade-offs**: May be slower than vectorized execution for analytics

#### ADR-006: Catalog-Storage Integration
- **Context**: How Catalog relates to TableHeap storage
- **Decision**: `TableInfo` struct holds `shared_ptr<TableHeap>` directly
- **Rationale**:
  - Eliminates separate registry for heaps
  - Creating a table automatically creates its storage
  - Single lookup to get both schema and storage
- **Trade-offs**: Catalog becomes larger; TableHeap lifetime tied to catalog entry

#### ADR-007: Nested Loop Join Implementation
- **Context**: Implementing JOIN operations for multi-table queries
- **Decision**: Nested Loop Join as the initial join algorithm
- **Rationale**:
  - Simplest join algorithm to implement correctly
  - Works with any join condition (not just equality)
  - No additional data structure requirements
  - Supports all join types (INNER, LEFT, RIGHT, CROSS)
- **Implementation Notes**:
  - For LEFT JOIN: tracks if left tuple found match, emits NULL-padded row if not
  - For RIGHT JOIN: materializes right side to track matched tuples
  - Combined output schema created at execution time
- **Trade-offs**: O(n×m) complexity; Hash Join needed for large tables

#### ADR-008: Hash-Based Aggregation
- **Context**: Implementing GROUP BY and aggregate functions efficiently
- **Decision**: Hash-based single-pass aggregation with FNV-1a key hashing
- **Rationale**:
  - O(n) scan with O(g) memory (g = groups) - optimal for large datasets
  - FNV-1a hash provides good distribution for composite keys
  - Incremental accumulators minimize memory per group
  - Single pass eliminates sort overhead for GROUP BY
- **Implementation Notes**:
  - Type promotion: SUM(INTEGER) → BIGINT to prevent overflow
  - AVG computed as sum/count at output time (not during accumulation)
  - Empty table edge case: COUNT(*)=0, SUM=NULL
- **Trade-offs**: Unordered output (ORDER BY would require separate sort)

#### ADR-009: Sort Executor (ORDER BY)
- **Context**: Implementing ORDER BY clause for result ordering
- **Decision**: std::sort (introsort) with custom comparator
- **Rationale**:
  - O(n log n) guaranteed time complexity
  - In-place sorting minimizes memory overhead
  - Custom comparator supports multi-column sort with mixed ASC/DESC
- **Implementation Notes**:
  - NULL handling: NULLs sort first in ASC, last in DESC (SQL standard)
  - Materialize-then-emit pattern for volcano model compatibility
  - Type-aware comparison: numeric, string, boolean
- **Trade-offs**: Requires full materialization (O(n) memory)

#### ADR-010: Limit Executor (LIMIT/OFFSET)
- **Context**: Implementing LIMIT and OFFSET clauses
- **Decision**: Streaming O(1) per-tuple implementation
- **Rationale**:
  - No materialization required - streams results
  - O(1) space complexity regardless of LIMIT value
  - OFFSET skipping without storing skipped tuples
- **Implementation Notes**:
  - Counter-based tracking (skipped, returned counts)
  - Early termination when LIMIT reached
  - Handles edge cases: OFFSET > row count, no LIMIT
- **Trade-offs**: Must be applied after ORDER BY (if present)

#### ADR-011: Hash Join (Equi-Joins)
- **Context**: Performance optimization for equi-joins (key = key)
- **Decision**: Classic build/probe hash join algorithm
- **Rationale**:
  - O(n + m) time complexity vs O(n × m) for nested loop
  - Optimal for large table joins on equality conditions
  - unordered_multimap handles duplicate keys efficiently
- **Implementation Notes**:
  - Build phase: Hash smaller (left) table into memory
  - Probe phase: Stream larger (right) table, O(1) lookups
  - FNV-1a inspired hashing for TupleValue keys
  - Supports INNER, LEFT, RIGHT joins
- **Trade-offs**: Requires build side to fit in memory

#### ADR-012: Index Scan Executor
- **Context**: Leveraging B+ tree indexes for efficient queries
- **Decision**: Three scan modes with B+ tree iterator integration
- **Rationale**:
  - Point lookup: O(log n) - optimal for equality conditions
  - Range scan: O(log n + k) - efficient for range predicates
  - Full scan: O(n) via leaf sibling pointers
- **Implementation Notes**:
  - Uses lower_bound() for O(log n) seek
  - RID-to-tuple lookup via TableHeap with buffer pool (O(1))
  - Graceful handling of deleted tuples (continue to next)
  - Three modes: POINT_LOOKUP, RANGE_SCAN, FULL_SCAN
- **Trade-offs**: Index must exist and be maintained

#### ADR-013: Page Checksums
- **Context**: Detecting torn writes and silent corruption in the page store
- **Decision**: CRC-32 (IEEE polynomial 0xEDB88320) stamped into the page header on write, verified on every read, on by default
- **Rationale**:
  - The disk manager rejects a mismatching page with an explicit `Status::Corruption` instead of returning garbage
  - An all-zero page is a valid empty page, so freshly extended files verify cleanly
  - Checksum lives at a fixed header offset and is excluded from its own computation
- **Trade-offs**: A CRC pass per page read. Detects corruption, does not repair it.

#### ADR-014: ARIES-lite Crash Recovery
- **Context**: Restoring a consistent, durable state after a crash
- **Decision**: WAL-based recovery in analysis, redo, and undo phases, anchored at the last checkpoint
- **Rationale**:
  - Analysis scans forward from the last CHECKPOINT to rebuild the loser set and the redo start LSN
  - Redo is page-LSN-gated: a record whose LSN is at or below the page LSN is skipped, so redo is idempotent
  - Undo rolls losers back and rollback emits compensation log records so re-runs stay safe
  - Checkpoints flush the WAL and dirty pages, then advance the redo anchor
- **Trade-offs**: Checkpoints are taken at startup and after DROP TABLE, not on a periodic timer.

#### ADR-015: Deterministic Crash Simulator
- **Context**: Proving recovery is correct under adversarial crash timing
- **Decision**: A FoundationDB-style single-threaded simulator that injects storage faults from a seed and replays them
- **Rationale**:
  - One 64-bit seed derives independent PRNG streams (SplitMix64) for the workload, page device, and log store, so a run is fully reproducible
  - `SimDiskManager` and `SimLogStore` resolve unsynced writes at a simulated crash to lost, torn, or durably-kept outcomes, and can inject transient write errors
  - Named schedules place the crash at specific points (for example between WAL flush and page flush)
  - After recovery an oracle checks invariants: every committed row present byte for byte, no uncommitted or rolled-back write visible
  - The `entropy-sim` CLI runs seed sweeps and writes one JSONL line per run
- **Trade-offs**: Models faults at the storage seam. It does not inject random in-page bit flips (checksums cover that on the read path).

---

## File Dependencies Graph

```
common/types.hpp
    └── (base types, no dependencies)

common/status.hpp
    └── common/types.hpp

storage/page.hpp
    ├── common/types.hpp
    └── common/config.hpp

storage/disk_manager.hpp
    ├── common/status.hpp
    └── storage/page.hpp

storage/buffer_pool.hpp
    ├── storage/disk_manager.hpp
    ├── storage/page.hpp
    └── storage/lru_replacer.hpp

storage/b_plus_tree.hpp
    ├── storage/buffer_pool.hpp
    └── storage/b_plus_tree_page.hpp

catalog/schema.hpp
    └── catalog/column.hpp

catalog/catalog.hpp
    ├── catalog/schema.hpp
    └── storage/buffer_pool.hpp

transaction/transaction.hpp
    └── common/types.hpp

transaction/wal.hpp
    ├── transaction/log_record.hpp
    └── storage/disk_manager.hpp

execution/executor.hpp
    ├── catalog/catalog.hpp
    ├── storage/table_heap.hpp
    └── transaction/transaction.hpp
```

---

## Performance Targets

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Bulk Insert | 50,000+ rows/sec | 1M row insert benchmark |
| Point Query (indexed) | < 1ms p99 | Random key lookup |
| Range Query | 100K+ rows/sec | Scan with predicate |
| Transaction Throughput | 10,000+ TPS | TPC-C style workload |
| Buffer Pool Hit Rate | > 95% | Working set fits in memory |

---

*Last Updated: 2026-07-11*
*Version: 0.1.1*
