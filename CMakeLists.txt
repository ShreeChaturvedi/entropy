cmake_minimum_required(VERSION 3.20)

project(entropy
    VERSION 0.1.1
    DESCRIPTION "High-performance relational database engine"
    LANGUAGES CXX
)

# ─────────────────────────────────────────────────────────────────────────────
# Global Settings
# ─────────────────────────────────────────────────────────────────────────────

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Export compile commands for IDE support
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Default to Release build if not specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'Release' as none was specified.")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Options
# ─────────────────────────────────────────────────────────────────────────────

option(ENTROPY_BUILD_TESTS "Build unit tests" ON)
option(ENTROPY_BUILD_BENCHMARKS "Build performance benchmarks" OFF)
option(ENTROPY_BENCH_COMPARE_SQLITE "Build SQLite comparison benchmarks" OFF)
option(ENTROPY_BUILD_EXAMPLES "Build example programs" ON)
option(ENTROPY_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENTROPY_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(ENTROPY_BUILD_TUI "Build the terminal UI (fetches FTXUI)" OFF)

# ─────────────────────────────────────────────────────────────────────────────
# CMake Modules
# ─────────────────────────────────────────────────────────────────────────────

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(CompilerWarnings)
include(Dependencies)

# ─────────────────────────────────────────────────────────────────────────────
# Compiler Flags
# ─────────────────────────────────────────────────────────────────────────────

# Apply compiler warnings
entropy_set_compiler_warnings()

# Sanitizers
if(ENTROPY_ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

if(ENTROPY_ENABLE_TSAN)
    add_compile_options(-fsanitize=thread)
    add_link_options(-fsanitize=thread)
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Install Layout
# ─────────────────────────────────────────────────────────────────────────────

# Included before the source tree so target-level install(TARGETS ...) rules in
# src/ can reference CMAKE_INSTALL_LIBDIR / INCLUDEDIR / BINDIR.
include(GNUInstallDirs)

# ─────────────────────────────────────────────────────────────────────────────
# Source Directories
# ─────────────────────────────────────────────────────────────────────────────

# Public headers
set(ENTROPY_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

# Generated headers (e.g. version.hpp derived from PROJECT_VERSION)
set(ENTROPY_GENERATED_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated/include")

configure_file(
    "${ENTROPY_INCLUDE_DIR}/entropy/version.hpp.in"
    "${ENTROPY_GENERATED_INCLUDE_DIR}/entropy/version.hpp"
    @ONLY
)

# Add source directory
add_subdirectory(src)

# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_BUILD_TESTS)
    enable_testing()
    include(CTest)
    add_subdirectory(tests)
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Benchmarks
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Examples
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Tools (entropy-sim crash-simulation runner)
# ─────────────────────────────────────────────────────────────────────────────

add_subdirectory(tools)

# ─────────────────────────────────────────────────────────────────────────────
# Terminal UI (optional; pulls FTXUI via FetchContent only when enabled)
# ─────────────────────────────────────────────────────────────────────────────

if(ENTROPY_BUILD_TUI)
    add_subdirectory(tui)
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Installation & Package Config
# ─────────────────────────────────────────────────────────────────────────────

include(CMakePackageConfigHelpers)

# Install location for the package config files, shared by the export rule, the
# config-file generation, and their install rules below.
set(ENTROPY_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/entropy")

# Public headers. Exclude the version.hpp.in template; the generated
# version.hpp is installed separately below.
install(
    DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    PATTERN "*.in" EXCLUDE
)

# Generated version header (configured into the build tree).
install(
    FILES "${ENTROPY_GENERATED_INCLUDE_DIR}/entropy/version.hpp"
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/entropy
)

# Library targets are added to the `entropyTargets` export set in src/. Install
# and export it so downstream projects can `find_package(entropy)` and link
# `entropy::entropy`.
install(EXPORT entropyTargets
    FILE entropyTargets.cmake
    NAMESPACE entropy::
    DESTINATION ${ENTROPY_INSTALL_CMAKEDIR}
)

configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/entropyConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/entropyConfig.cmake"
    INSTALL_DESTINATION ${ENTROPY_INSTALL_CMAKEDIR}
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/entropyConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    # Pre-1.0: treat the minor version as the compatibility boundary so a
    # consumer pinning 0.1 does not silently accept a breaking 0.2.
    COMPATIBILITY SameMinorVersion
)

install(
    FILES
        "${CMAKE_CURRENT_BINARY_DIR}/entropyConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/entropyConfigVersion.cmake"
    DESTINATION ${ENTROPY_INSTALL_CMAKEDIR}
)

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

message(STATUS "")
message(STATUS "Entropy Database Engine v${PROJECT_VERSION}")
message(STATUS "─────────────────────────────────────────────")
message(STATUS "Build type:        ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Compiler:      ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "C++ Standard:      ${CMAKE_CXX_STANDARD}")
message(STATUS "")
message(STATUS "Options:")
message(STATUS "  Tests:           ${ENTROPY_BUILD_TESTS}")
message(STATUS "  Benchmarks:      ${ENTROPY_BUILD_BENCHMARKS}")
message(STATUS "  Bench SQLite:    ${ENTROPY_BENCH_COMPARE_SQLITE}")
message(STATUS "  Examples:        ${ENTROPY_BUILD_EXAMPLES}")
message(STATUS "  Terminal UI:     ${ENTROPY_BUILD_TUI}")
message(STATUS "  AddressSanitizer:${ENTROPY_ENABLE_ASAN}")
message(STATUS "  ThreadSanitizer: ${ENTROPY_ENABLE_TSAN}")
message(STATUS "")
