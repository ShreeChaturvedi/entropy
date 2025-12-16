# ─────────────────────────────────────────────────────────────────────────────
# Entropy Database Engine - Benchmarks
# ─────────────────────────────────────────────────────────────────────────────

# Insert benchmark
add_executable(insert_benchmark
    insert_benchmark.cpp
)

target_link_libraries(insert_benchmark
    PRIVATE
        entropy
        benchmark::benchmark
        benchmark::benchmark_main
)

# Query benchmark
add_executable(query_benchmark
    query_benchmark.cpp
)

target_link_libraries(query_benchmark
    PRIVATE
        entropy
        benchmark::benchmark
        benchmark::benchmark_main
)
