# Third-Party Dependencies

This directory is reserved for external dependencies that cannot be fetched
via CMake FetchContent.

## Dependencies Managed via FetchContent

The following dependencies are automatically downloaded during the build:

| Library | Version | Purpose |
|---------|---------|---------|
| spdlog | 1.12.0 | Logging |
| Google Test | 1.14.0 | Unit testing |
| Google Benchmark | 1.8.3 | Performance benchmarks |

## Manual Dependencies

If you need to use a local copy of any dependency instead of FetchContent:

1. Clone or copy the dependency into this directory
2. Modify `cmake/Dependencies.cmake` to use the local path instead of FetchContent

## SQL Parser

The SQL parser library (hsql) integration is pending. When implemented:

1. The library will be fetched via FetchContent
2. Or you can clone it manually: `git clone https://github.com/hyrise/sql-parser.git`
