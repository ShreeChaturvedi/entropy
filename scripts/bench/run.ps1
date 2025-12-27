param(
  [string]$BuildDir = "build/bench"
)

$RootDir = Resolve-Path "$PSScriptRoot/../.."
$RunDir = Join-Path $RootDir "docs/benchmarks/runs"
$CompareSqlite = $env:ENTROPY_BENCH_COMPARE_SQLITE
if ([string]::IsNullOrWhiteSpace($CompareSqlite)) {
  $CompareSqlite = "ON"
}

New-Item -ItemType Directory -Force -Path $RunDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$OutJson = Join-Path $RunDir "bench-$Timestamp.json"

cmake -S $RootDir -B $BuildDir -DCMAKE_BUILD_TYPE=Release `
  -DENTROPY_BUILD_BENCHMARKS=ON `
  -DENTROPY_BENCH_COMPARE_SQLITE=$CompareSqlite `
  -DENTROPY_BUILD_TESTS=OFF `
  -DENTROPY_BUILD_EXAMPLES=OFF

$CacheFile = Join-Path $BuildDir "CMakeCache.txt"
if ($CompareSqlite -eq "ON") {
  $sqliteFound = Select-String -Path $CacheFile -Pattern "SQLite3_FOUND:BOOL=(1|TRUE)" -Quiet
  $sqliteLibFound = Select-String -Path $CacheFile -Pattern "SQLite3_LIBRARY:FILEPATH=" -Quiet
  $sqliteIncFound = Select-String -Path $CacheFile -Pattern "SQLite3_INCLUDE_DIR:PATH=" -Quiet

  if (-Not $sqliteFound -and (-Not $sqliteLibFound -or -Not $sqliteIncFound)) {
    Write-Error "SQLite3 not found. Install sqlite3 dev headers or set ENTROPY_BENCH_COMPARE_SQLITE=OFF."
    exit 1
  }
}

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
