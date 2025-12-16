# Entropy

A high-performance relational database engine built from scratch in modern C++20.

Entropy is a fully-featured RDBMS implementation demonstrating core database internals including a custom B+ tree storage engine, MVCC-based transaction management, cost-based query optimization, and ACID-compliant transaction processing with write-ahead logging.

## Features

### Storage Engine
- **B+ Tree Indexes** - Disk-based B+ tree implementation with efficient range scans
- **Hash Indexes** - O(1) point lookups using extendible hashing
- **Buffer Pool Manager** - Page caching with LRU eviction policy
- **Slotted Page Format** - Variable-length record support

### Transaction Processing
- **MVCC** - Multi-version concurrency control for snapshot isolation
- **ACID Compliance** - Full atomicity, consistency, isolation, and durability guarantees
- **Write-Ahead Logging** - Crash recovery with REDO/UNDO logging
- **Lock Manager** - Two-phase locking with deadlock detection

### Query Processing
- **SQL Support** - SELECT, INSERT, UPDATE, DELETE with JOINs
- **Join Algorithms** - Nested loop join and hash join implementations
- **Aggregations** - COUNT, SUM, AVG, MIN, MAX, GROUP BY
- **Query Optimizer** - Cost-based optimization with statistics

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        SQL Interface                            │
├─────────────────────────────────────────────────────────────────┤
│                     Parser → Binder → Optimizer                 │
├─────────────────────────────────────────────────────────────────┤
│                     Execution Engine                            │
│         (SeqScan, IndexScan, HashJoin, Aggregate, etc.)        │
├─────────────────────────────────────────────────────────────────┤
│              Transaction Manager (MVCC + WAL)                   │
├─────────────────────────────────────────────────────────────────┤
│                   Catalog (Schema Metadata)                     │
├─────────────────────────────────────────────────────────────────┤
│               Storage (B+ Tree, Table Heap, Tuple)              │
├─────────────────────────────────────────────────────────────────┤
│                  Buffer Pool (LRU Cache)                        │
├─────────────────────────────────────────────────────────────────┤
│                     Disk Manager                                │
└─────────────────────────────────────────────────────────────────┘
```

## Building

### Prerequisites
- C++20 compatible compiler (GCC 10+, Clang 12+, MSVC 19.29+)
- CMake 3.20 or higher
- Git

### Build Instructions

```bash
# Clone the repository
git clone https://github.com/yourusername/entropy.git
cd entropy

# Create build directory
mkdir build && cd build

# Configure and build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# Run tests
ctest --output-on-failure
```

### Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `ENTROPY_BUILD_TESTS` | ON | Build unit tests |
| `ENTROPY_BUILD_BENCHMARKS` | OFF | Build performance benchmarks |
| `ENTROPY_ENABLE_LZ4` | OFF | Enable page compression |

## Quick Start

```cpp
#include <entropy/entropy.hpp>

int main() {
    // Open or create a database
    entropy::Database db("mydb.entropy");

    // Execute SQL
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");

    // Query with results
    auto result = db.execute("SELECT * FROM users WHERE id = 1");
    for (const auto& row : result) {
        std::cout << row["name"].as_string() << std::endl;
    }

    return 0;
}
```

## Project Structure

```
entropy/
├── include/entropy/     # Public API headers
├── src/
│   ├── common/          # Shared utilities and types
│   ├── storage/         # Storage engine (pages, B+ tree, buffer pool)
│   ├── catalog/         # Schema and metadata management
│   ├── transaction/     # MVCC, WAL, lock manager
│   ├── execution/       # Query executors
│   ├── optimizer/       # Query optimizer
│   ├── parser/          # SQL parsing
│   └── api/             # Public API implementation
├── tests/               # Unit and integration tests
├── benchmarks/          # Performance benchmarks
└── examples/            # Example usage
```

## Technical Highlights

### B+ Tree Implementation
- Supports variable-length keys
- Efficient range scans with leaf node linking
- Node splitting and merging for balanced tree maintenance
- Configurable fan-out for performance tuning

### Buffer Pool Manager
- LRU replacement policy with pin counting
- Dirty page tracking for efficient flushing
- Thread-safe page access with latching

### MVCC
- Snapshot isolation for consistent reads
- Version chains for concurrent access
- Garbage collection of old versions

### Write-Ahead Logging
- ARIES-style recovery protocol
- Group commit optimization
- Checkpointing for faster recovery

## Documentation

- [Design Document](DESIGN.md) - Architecture decisions and component details
- [Progress Tracker](PROGRESS.md) - Development status and roadmap

## Testing

```bash
# Run all tests
cd build && ctest --output-on-failure

# Run specific test suite
./tests/storage_tests
./tests/transaction_tests

# Run with verbose output
ctest -V
```

## Contributing

This is a personal project for learning and demonstration purposes. If you find bugs or have suggestions, feel free to open an issue.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

This project was inspired by:
- [CMU 15-445/645 Database Systems](https://15445.courses.cs.cmu.edu/)
- [SQLite](https://www.sqlite.org/)
- [LevelDB](https://github.com/google/leveldb)

---

*Built with modern C++20 to demonstrate database internals and systems programming.*
