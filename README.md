<p align="center">
  <img src="docs/branding/readme-light.svg#gh-light-mode-only" width="860" alt="Entropy Database Engine">
  <img src="docs/branding/readme-dark.svg#gh-dark-mode-only" width="860" alt="Entropy Database Engine">
</p>

---

[![ci](https://github.com/ShreeChaturvedi/entropy/actions/workflows/ci.yml/badge.svg)](https://github.com/ShreeChaturvedi/entropy/actions/workflows/ci.yml)
[![release](https://img.shields.io/github/v/release/ShreeChaturvedi/entropy?include_prereleases)](https://github.com/ShreeChaturvedi/entropy/releases)
[![license](https://img.shields.io/github/license/ShreeChaturvedi/entropy)](LICENSE)
[![c++](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://isocpp.org/)

Entropy is a high-performance relational database engine built from scratch in
modern C++20. It showcases core database internals: B+ tree storage, MVCC,
ACID transactions, write-ahead logging, and a cost-based query optimizer.

## Highlights

- End-to-end SQL engine: parser, binder, optimizer, execution.
- Storage engine with B+ trees, hash indexes, and buffer pool caching.
- MVCC + WAL for snapshot isolation and crash recovery.
- Unit and integration tests wired to CTest + GoogleTest.
- Benchmarks with optional SQLite comparison and reproducible scripts.
- Cross-platform CMake build with Linux/macOS/Windows CI.

## Architecture

<p align="center">
  <img src="docs/diagrams/architecture-light.svg#gh-light-mode-only" width="860" alt="Entropy architecture diagram">
  <img src="docs/diagrams/architecture-dark.svg#gh-dark-mode-only" width="860" alt="Entropy architecture diagram">
</p>

## Architecture Details

Entropy is organized as focused C++ libraries that mirror the logical
database pipeline. The implementation favors explicit data structures and
clear boundaries between planning, execution, concurrency, and storage.

- SQL front-end: custom lexer/parser builds an AST; the binder resolves
  names, types, and schema references with optional strict mode checks.
- Optimizer: statistics-driven cost model builds plan nodes and chooses
  index access paths (B+ tree for ranges, extendible hash for point lookups).
- Execution engine: iterator-style operators for scans, joins, aggregates,
  sorting, filtering, projection, and DML (insert/update/delete).
- Concurrency + recovery: MVCC version chains with a lock manager for
  transactional isolation; write-ahead logging with recovery replay.
- Storage engine: table heap on slotted pages for variable-length tuples,
  B+ tree indexes for ordered access, and extendible hash indexes.
- Buffer pool: page table + LRU replacer with pin/dirty tracking; disk
  manager handles page file and WAL I/O.

## Performance Snapshot (vs SQLite)

Run file: `docs/benchmarks/runs/bench-20251226-211444.json`

- Machine: Apple M2 (arm64), macOS 15.5
- Compiler: Apple clang 17.0.0 (clang-1700.0.13.5)
- Build: Release (`-O3`)
SQLite baselines are collected when `ENTROPY_BENCH_COMPARE_SQLITE=ON`.

Per-iteration ns/op (ratio = Entropy / SQLite, lower is better):

| Case | Rows | Entropy (ns/op) | SQLite (ns/op) | Ratio |
| --- | --- | --- | --- | --- |
| Insert batch (txn) | 1k | `1,137,350` | `968,605` | `1.17x` |
| Insert batch (txn) | 10k | `38,729,880` | `6,951,009` | `5.57x` |
| Point select | 1k | `225,263` | `22,670` | `9.94x` |
| Point select | 10k | `2,179,541` | `180,032` | `12.11x` |

Rows = batch size for inserts; table cardinality for point selects.

Full results: `docs/benchmarks/bench_summary.csv`

## Benchmarks

```bash
./scripts/bench/run.sh
```

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/bench/run.ps1
```

The script writes a timestamped JSON run file in `docs/benchmarks/runs/` and
updates `docs/benchmarks/bench_summary.csv`.

SQLite comparison is ON by default for the script. If SQLite3 headers are
missing, the script fails with an explicit error. To run without SQLite,
set `ENTROPY_BENCH_COMPARE_SQLITE=OFF`. Manual steps and methodology are in
`docs/benchmarks.md`.

## Quick Start

```cpp
#include <entropy/entropy.hpp>
#include <iostream>

int main() {
    entropy::Database db("mydb.entropy");
    db.execute("CREATE TABLE users (id INT, name VARCHAR(100))");
    db.execute("INSERT INTO users VALUES (1, 'Alice')");

    auto result = db.execute("SELECT * FROM users");
    for (const auto &row : result) {
        std::cout << row["name"].as_string() << "\n";
    }
}
```

## Build and Test

### Prerequisites

- C++20 compiler (GCC 10+, Clang 12+, MSVC 19.29+)
- CMake 3.20+
- Git

### Configure + Build

Dependencies (spdlog, GoogleTest, Google Benchmark) are fetched with CMake
FetchContent on first configure (requires network access). Optional
comparisons use system SQLite3, while compression uses LZ4 when enabled.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

### Run Tests

```bash
ctest --test-dir build --output-on-failure -C Release
```

### CMake Presets

```bash
cmake --preset dev
cmake --build --preset dev
ctest --preset dev
```

## Build Options

| Option | Default | Description |
| --- | :---: | --- |
| `ENTROPY_BUILD_TESTS` | &#10003; | Build unit + integration tests |
| `ENTROPY_BUILD_BENCHMARKS` | &#10007; | Build benchmarks |
| `ENTROPY_BENCH_COMPARE_SQLITE` | &#10007; | Build SQLite comparison benchmarks |
| `ENTROPY_BUILD_EXAMPLES` | &#10003; | Build example programs |
| `ENTROPY_ENABLE_LZ4` | &#10007; | Enable page compression (LZ4) |

LZ4 compression tests are only built when `ENTROPY_ENABLE_LZ4=ON`.

## CI/CD and Releases

- CI builds and tests on Linux/macOS/Windows via GitHub Actions.
- Releases are created automatically on `v*` tags with OS-specific binaries.

## Documentation

- `DESIGN.md` -- architecture notes and component details
- `docs/benchmarks.md` -- benchmark methodology and reporting

## License

MIT -- see `LICENSE`.
