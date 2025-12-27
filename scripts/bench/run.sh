#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
build_dir="${1:-$root_dir/build/bench}"
run_dir="$root_dir/docs/benchmarks/runs"
compare_sqlite="${ENTROPY_BENCH_COMPARE_SQLITE:-ON}"

mkdir -p "$run_dir"

ts=$(date +"%Y%m%d-%H%M%S")
out_json="$run_dir/bench-$ts.json"

cmake -S "$root_dir" -B "$build_dir" -DCMAKE_BUILD_TYPE=Release \
  -DENTROPY_BUILD_BENCHMARKS=ON \
  -DENTROPY_BENCH_COMPARE_SQLITE="$compare_sqlite" \
  -DENTROPY_BUILD_TESTS=OFF \
  -DENTROPY_BUILD_EXAMPLES=OFF

cache_file="$build_dir/CMakeCache.txt"
if [[ "$compare_sqlite" == "ON" ]]; then
  if command -v rg >/dev/null 2>&1; then
    rg -q "SQLite3_FOUND:BOOL=(1|TRUE)" "$cache_file" || sqlite_ok=0
    rg -q "SQLite3_LIBRARY:FILEPATH=" "$cache_file" || sqlite_lib_ok=0
    rg -q "SQLite3_INCLUDE_DIR:PATH=" "$cache_file" || sqlite_inc_ok=0
  else
    grep -Eq "SQLite3_FOUND:BOOL=(1|TRUE)" "$cache_file" || sqlite_ok=0
    grep -Eq "SQLite3_LIBRARY:FILEPATH=" "$cache_file" || sqlite_lib_ok=0
    grep -Eq "SQLite3_INCLUDE_DIR:PATH=" "$cache_file" || sqlite_inc_ok=0
  fi

  if [[ "${sqlite_ok:-1}" -eq 0 && ("${sqlite_lib_ok:-1}" -eq 0 || "${sqlite_inc_ok:-1}" -eq 0) ]]; then
    echo "SQLite3 not found. Install sqlite3 dev headers or set ENTROPY_BENCH_COMPARE_SQLITE=OFF." >&2
    exit 1
  fi
fi

cmake --build "$build_dir" --config Release

"$build_dir/benchmarks/entropy_bench" \
  --benchmark_format=json \
  --benchmark_out="$out_json"

python3 "$root_dir/scripts/bench/summarize.py" \
  "$out_json" \
  "$root_dir/docs/benchmarks/bench_summary.csv"

echo "Benchmark run saved to $out_json"
