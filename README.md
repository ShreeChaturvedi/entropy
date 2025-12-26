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

## Performance Snapshot

Run file: `docs/benchmarks/runs/bench-<timestamp>.json`

- Machine: _populate from your benchmark run_
- Compiler: _populate from your benchmark run_
- Build: Release (`-O3`)

Median ns/op (ratio = Entropy / SQLite):

| Case | Entropy (ns/op) | SQLite (ns/op) | Ratio |
| --- | --- | --- | --- |
| Insert batch (1k rows, txn) | `TBD` | `TBD` | `TBD` |
| Point select (10k rows) | `TBD` | `TBD` | `TBD` |

Full results: `docs/benchmarks/bench_summary.csv`

## Benchmarks

```bash
cmake --preset bench
cmake --build --preset bench
./build/bench/benchmarks/entropy_bench --benchmark_format=json \
  --benchmark_out=docs/benchmarks/runs/bench-<timestamp>.json
python3 scripts/bench/summarize.py \
  docs/benchmarks/runs/bench-<timestamp>.json \
  docs/benchmarks/bench_summary.csv
```

SQLite comparison requires `ENTROPY_BENCH_COMPARE_SQLITE=ON` and a system
SQLite3 development package.

Detailed methodology and scripts: `docs/benchmarks.md` and `scripts/bench/`.

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
| --- | --- | --- |
| `ENTROPY_BUILD_TESTS` | ON | Build unit + integration tests |
| `ENTROPY_BUILD_BENCHMARKS` | OFF | Build benchmarks |
| `ENTROPY_BENCH_COMPARE_SQLITE` | OFF | Build SQLite comparison benchmarks |
| `ENTROPY_BUILD_EXAMPLES` | ON | Build example programs |
| `ENTROPY_ENABLE_LZ4` | OFF | Enable page compression (LZ4) |

LZ4 compression tests are only built when `ENTROPY_ENABLE_LZ4=ON`.

## CI/CD and Releases

- CI builds and tests on Linux/macOS/Windows via GitHub Actions.
- Releases are created automatically on `v*` tags with OS-specific binaries.

## Documentation

- `DESIGN.md` -- architecture notes and component details
- `docs/benchmarks.md` -- benchmark methodology and reporting
- `PROGRESS.md` -- roadmap and status updates

## License

MIT -- see `LICENSE`.
