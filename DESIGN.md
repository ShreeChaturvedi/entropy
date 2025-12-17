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
- Custom B+ tree storage engine
- MVCC-based transaction management
- Cost-based query optimization
- ACID compliance with WAL

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
| Compression | LZ4 (optional) | Fast compression for pages |
| Logging | spdlog | Header-only, fast, modern |

---

## Directory Structure

```
entropy/
├── CMakeLists.txt              # Root CMake configuration
├── README.md                   # Project overview for GitHub
├── DESIGN.md                   # This file - architecture decisions
├── PROGRESS.md                 # Development progress tracking
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
│   │   ├── tuple.hpp           # Row/tuple representation
│   │   └── tuple.cpp
│   │
│   ├── catalog/                # Schema/metadata management
│   │   ├── CMakeLists.txt
│   │   ├── catalog.hpp         # System catalog
│   │   ├── catalog.cpp
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
│   │   ├── lock_manager.hpp    # Lock management (2PL)
│   │   ├── lock_manager.cpp
│   │   ├── mvcc.hpp            # MVCC implementation
│   │   ├── mvcc.cpp
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
│   └── api/                    # Public API implementation
│       ├── CMakeLists.txt
│       ├── database.cpp        # Database class impl
│       ├── result.cpp          # Result set handling
│       └── shell.cpp           # Interactive SQL shell
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
│   ├── format.sh               # Code formatting script
│   └── coverage.sh             # Coverage report script
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
| HashIndex | ⭕ Not Started | stub | stub | - |
| TableHeap | ✅ Complete | `table_heap.hpp` | `table_heap.cpp` | 23 passing |
| Tuple | ✅ Complete | `tuple.hpp` | `tuple.cpp` | 46 passing |

### Catalog Layer
| Component | Status | Header | Implementation | Tests |
|-----------|--------|--------|----------------|-------|
| Catalog | ✅ Complete | `catalog.hpp` | `catalog.cpp` | 8 passing |
| Schema | ✅ Complete | `schema.hpp` | `schema.cpp` | included |
| Column | ✅ Complete | `column.hpp` | `column.cpp` | included |

### Transaction Layer
| Component | Status | Header | Implementation | Tests |
|-----------|--------|--------|----------------|-------|
| Transaction | ✅ Complete | `transaction.hpp` | `transaction.cpp` | 8 passing |
| TransactionManager | ✅ Complete | `transaction_manager.hpp` | `transaction_manager.cpp` | 11 passing |
| LockManager | ✅ Complete | `lock_manager.hpp` | `lock_manager.cpp` | 26 passing |
| MVCC | ✅ Complete | `mvcc.hpp` | `mvcc.cpp` | 11 passing |
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
| NestedLoopJoin | ⭕ Not Started | stub | stub | - |
| HashJoin | ⭕ Not Started | stub | stub | - |
| Aggregation | ⭕ Not Started | stub | stub | - |
| IndexScanExecutor | ⭕ Not Started | stub | stub | - |

### Optimizer Layer
| Component | Status | Header | Implementation | Tests |
|-----------|--------|--------|----------------|-------|
| Optimizer | ⭕ Not Started | stub | stub | - |
| CostModel | ⭕ Not Started | stub | stub | - |
| Statistics | ⭕ Not Started | stub | stub | - |
| IndexSelector | ⭕ Not Started | stub | stub | - |

### API Layer
| Component | Status | Header | Implementation | Tests |
|-----------|--------|--------|----------------|-------|
| Database | ✅ Complete | `database.hpp` | `database.cpp` | 16 passing |
| Result | ✅ Complete | `result.hpp` | `result.cpp` | included |
| Shell | 🟡 Partial | - | `shell.cpp` | - |

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
1. **Buffer Pool**: Thread-safe with per-page latches (shared/exclusive)
2. **B+ Tree**: Latch crabbing protocol for concurrent access
3. **Transaction Manager**: Centralized lock manager with 2PL + MVCC
4. **WAL**: Single-writer with group commit optimization

### Lock Hierarchy (to prevent deadlocks)
1. Lock Manager mutex
2. Transaction mutexes
3. Buffer pool frame latches
4. Page latches (acquired via crabbing)

---

## External Dependencies

| Library | Version | Purpose | Integration |
|---------|---------|---------|-------------|
| Google Test | 1.14+ | Unit testing | FetchContent |
| Google Benchmark | 1.8+ | Performance benchmarks | FetchContent |
| spdlog | 1.12+ | Logging | FetchContent |
| hsql | latest | SQL parsing | FetchContent or submodule |
| LZ4 | 1.9+ | Page compression (optional) | System or FetchContent |

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
- **Decision**: B+ tree as primary, hash index for equality (planned)
- **Consequences**: B+ tree handles range queries, hash for O(1) point lookups

#### ADR-003: Concurrency Control
- **Context**: How to handle concurrent transactions
- **Decision**: MVCC with snapshot isolation + 2PL lock manager
- **Consequences**: Readers don't block writers, requires garbage collection of old versions

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

*Last Updated: [Date of last update]*
*Version: 0.1.0*
