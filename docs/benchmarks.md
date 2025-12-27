# Benchmarks

This repo uses Google Benchmark to measure end-to-end SQL execution time for
Entropy and (optionally) SQLite. The focus is on realistic, single-connection
workloads that include SQL parsing, execution, and disk-backed persistence.

## Workloads

- Insert batch: insert N rows in a single transaction.
- Point select: query a single row from a table with N rows.

SQLite comparisons are compiled only when `ENTROPY_BENCH_COMPARE_SQLITE=ON`
**and** a system SQLite3 development package is available.

## Quick Start

```bash
./scripts/bench/run.sh
```

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/bench/run.ps1
```

The script writes a timestamped JSON run file in `docs/benchmarks/runs/` and
updates `docs/benchmarks/bench_summary.csv`. If SQLite3 is not available and
`ENTROPY_BENCH_COMPARE_SQLITE=ON`, the script exits with a clear error so the
comparison is not silently skipped.

## Run Benchmarks (Unix)

```bash
cmake -S . -B build/bench -DCMAKE_BUILD_TYPE=Release \
  -DENTROPY_BUILD_BENCHMARKS=ON \
  -DENTROPY_BENCH_COMPARE_SQLITE=ON \
  -DENTROPY_BUILD_TESTS=OFF \
  -DENTROPY_BUILD_EXAMPLES=OFF
cmake --build build/bench --config Release

./build/bench/benchmarks/entropy_bench \
  --benchmark_format=json \
  --benchmark_out=docs/benchmarks/runs/bench-<timestamp>.json

python3 scripts/bench/summarize.py \
  docs/benchmarks/runs/bench-<timestamp>.json \
  docs/benchmarks/bench_summary.csv
```

## Run Benchmarks (Windows)

```powershell
cmake -S . -B build/bench -DCMAKE_BUILD_TYPE=Release `
  -DENTROPY_BUILD_BENCHMARKS=ON `
  -DENTROPY_BENCH_COMPARE_SQLITE=ON `
  -DENTROPY_BUILD_TESTS=OFF `
  -DENTROPY_BUILD_EXAMPLES=OFF
cmake --build build/bench --config Release

./build/bench/benchmarks/Release/entropy_bench.exe `
  --benchmark_format=json `
  --benchmark_out=docs/benchmarks/runs/bench-<timestamp>.json

python scripts/bench/summarize.py `
  docs/benchmarks/runs/bench-<timestamp>.json `
  docs/benchmarks/bench_summary.csv
```

## Metrics

The summary captures per-iteration `real_time` in `ns/op` from Google
Benchmark. This is the mean time per benchmark iteration. If you want
distribution stats (p50/p95/p99), run with `--benchmark_repetitions` and
update `summarize.py` to read aggregate entries or compute percentiles.

## Reporting

Capture and publish:

- CPU model, RAM size, OS version
- Compiler and version
- Build type and flags
- Benchmark run JSON + summary CSV

See `docs/benchmarks/bench_summary.csv` for the expected schema.
