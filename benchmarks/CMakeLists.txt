# ─────────────────────────────────────────────────────────────────────────────
# Entropy Database Engine - Benchmarks
# ─────────────────────────────────────────────────────────────────────────────

# Benchmarks
add_executable(entropy_bench
    insert_benchmark.cpp
    query_benchmark.cpp
)

target_link_libraries(entropy_bench
    PRIVATE
        entropy
        benchmark::benchmark
        benchmark::benchmark_main
)

if(TARGET entropy_sqlite)
    target_link_libraries(entropy_bench PRIVATE entropy_sqlite)
endif()
