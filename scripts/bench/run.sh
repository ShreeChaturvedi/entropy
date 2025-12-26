#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
build_dir="${1:-$root_dir/build/bench}"
run_dir="$root_dir/docs/benchmarks/runs"

mkdir -p "$run_dir"

ts=$(date +"%Y%m%d-%H%M%S")
out_json="$run_dir/bench-$ts.json"

cmake -S "$root_dir" -B "$build_dir" -DCMAKE_BUILD_TYPE=Release \
  -DENTROPY_BUILD_BENCHMARKS=ON \
  -DENTROPY_BENCH_COMPARE_SQLITE=ON \
  -DENTROPY_BUILD_TESTS=OFF \
  -DENTROPY_BUILD_EXAMPLES=OFF

cmake --build "$build_dir" --config Release

"$build_dir/benchmarks/entropy_bench" \
  --benchmark_format=json \
  --benchmark_out="$out_json"

python3 "$root_dir/scripts/bench/summarize.py" \
  "$out_json" \
  "$root_dir/docs/benchmarks/bench_summary.csv"

echo "Benchmark run saved to $out_json"
