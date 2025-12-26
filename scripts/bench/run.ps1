param(
  [string]$BuildDir = "build/bench"
)

$RootDir = Resolve-Path "$PSScriptRoot/../.."
$RunDir = Join-Path $RootDir "docs/benchmarks/runs"

New-Item -ItemType Directory -Force -Path $RunDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$OutJson = Join-Path $RunDir "bench-$Timestamp.json"

cmake -S $RootDir -B $BuildDir -DCMAKE_BUILD_TYPE=Release `
  -DENTROPY_BUILD_BENCHMARKS=ON `
  -DENTROPY_BENCH_COMPARE_SQLITE=ON `
  -DENTROPY_BUILD_TESTS=OFF `
  -DENTROPY_BUILD_EXAMPLES=OFF

cmake --build $BuildDir --config Release

$ExePath = Join-Path $BuildDir "benchmarks/Release/entropy_bench.exe"
if (-Not (Test-Path $ExePath)) {
  $ExePath = Join-Path $BuildDir "benchmarks/entropy_bench.exe"
}

& $ExePath --benchmark_format=json --benchmark_out=$OutJson

python $RootDir/scripts/bench/summarize.py `
  $OutJson `
  $RootDir/docs/benchmarks/bench_summary.csv

Write-Host "Benchmark run saved to $OutJson"
