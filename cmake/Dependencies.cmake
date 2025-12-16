# ─────────────────────────────────────────────────────────────────────────────
# External Dependencies Configuration
# ─────────────────────────────────────────────────────────────────────────────

include(FetchContent)

# Set download directory for FetchContent
set(FETCHCONTENT_BASE_DIR "${CMAKE_BINARY_DIR}/_deps")

# ─────────────────────────────────────────────────────────────────────────────
# spdlog - Fast C++ logging library
# ─────────────────────────────────────────────────────────────────────────────

FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.12.0
    GIT_SHALLOW    TRUE
)

set(SPDLOG_BUILD_EXAMPLE OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SPDLOG_INSTALL OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(spdlog)

# ─────────────────────────────────────────────────────────────────────────────
# Google Test - Testing framework
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_BUILD_TESTS)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.14.0
        GIT_SHALLOW    TRUE
    )

    # For Windows: Prevent overriding the parent project's compiler/linker settings
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    set(BUILD_GMOCK ON CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)

    FetchContent_MakeAvailable(googletest)

    # Disable warnings for googletest
    if(TARGET gtest)
        entropy_disable_warnings(gtest)
    endif()
    if(TARGET gmock)
        entropy_disable_warnings(gmock)
    endif()
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Google Benchmark - Micro-benchmarking library
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_BUILD_BENCHMARKS)
    FetchContent_Declare(
        benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG        v1.8.3
        GIT_SHALLOW    TRUE
    )

    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_INSTALL_DOCS OFF CACHE BOOL "" FORCE)

    FetchContent_MakeAvailable(benchmark)

    if(TARGET benchmark)
        entropy_disable_warnings(benchmark)
    endif()
endif()

# ─────────────────────────────────────────────────────────────────────────────
# sql-parser (hsql) - SQL parsing library
# ─────────────────────────────────────────────────────────────────────────────

FetchContent_Declare(
    sqlparser
    GIT_REPOSITORY https://github.com/hyrise/sql-parser.git
    GIT_TAG        master
    GIT_SHALLOW    TRUE
)

# Note: hsql uses a different build approach, we'll handle it separately
# For now, we'll set up the declaration - implementation can be adjusted later

# ─────────────────────────────────────────────────────────────────────────────
# LZ4 - Fast compression library (optional)
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_ENABLE_LZ4)
    find_package(lz4 QUIET)

    if(NOT lz4_FOUND)
        FetchContent_Declare(
            lz4
            GIT_REPOSITORY https://github.com/lz4/lz4.git
            GIT_TAG        v1.9.4
            GIT_SHALLOW    TRUE
            SOURCE_SUBDIR  build/cmake
        )

        set(LZ4_BUILD_CLI OFF CACHE BOOL "" FORCE)
        set(LZ4_BUILD_LEGACY_LZ4C OFF CACHE BOOL "" FORCE)

        FetchContent_MakeAvailable(lz4)
    endif()

    add_compile_definitions(ENTROPY_ENABLE_COMPRESSION)
endif()
