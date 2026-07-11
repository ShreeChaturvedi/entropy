# ─────────────────────────────────────────────────────────────────────────────
# Entropy Database Engine - Source Build Configuration
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Common Library
# ─────────────────────────────────────────────────────────────────────────────

add_library(entropy_common STATIC
    common/config.cpp
    common/status.cpp
    common/logger.cpp
)

target_include_directories(entropy_common
    PUBLIC
        $<BUILD_INTERFACE:${ENTROPY_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(entropy_common
    PUBLIC
        spdlog::spdlog
)

# ─────────────────────────────────────────────────────────────────────────────
# Storage Library
# ─────────────────────────────────────────────────────────────────────────────

add_library(entropy_storage STATIC
    storage/page.cpp
    storage/disk_manager.cpp
    storage/buffer_pool.cpp
    storage/lru_replacer.cpp
    storage/tuple.cpp
    storage/table_page.cpp
    storage/table_heap.cpp
    storage/b_plus_tree.cpp
)

target_include_directories(entropy_storage
    PUBLIC
        $<BUILD_INTERFACE:${ENTROPY_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(entropy_storage
    PUBLIC
        entropy_common
)

# ─────────────────────────────────────────────────────────────────────────────
# Catalog Library
# ─────────────────────────────────────────────────────────────────────────────

add_library(entropy_catalog STATIC
    catalog/column.cpp
    catalog/schema.cpp
    catalog/catalog.cpp
    catalog/catalog_manifest.cpp
)

target_include_directories(entropy_catalog
    PUBLIC
        $<BUILD_INTERFACE:${ENTROPY_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(entropy_catalog
    PUBLIC
        entropy_common
        entropy_storage
)

# ─────────────────────────────────────────────────────────────────────────────
# Transaction Library
# ─────────────────────────────────────────────────────────────────────────────

add_library(entropy_transaction STATIC
    transaction/transaction.cpp
    transaction/transaction_manager.cpp
    transaction/lock_manager.cpp
    transaction/log_record.cpp
    transaction/version_store.cpp
    transaction/wal.cpp
    transaction/recovery.cpp
    transaction/mvcc.cpp
)

target_include_directories(entropy_transaction
    PUBLIC
        $<BUILD_INTERFACE:${ENTROPY_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(entropy_transaction
    PUBLIC
        entropy_common
        entropy_storage
)

# ─────────────────────────────────────────────────────────────────────────────
# Parser Library
# ─────────────────────────────────────────────────────────────────────────────

add_library(entropy_parser STATIC
    parser/token.cpp
    parser/expression.cpp
    parser/parser.cpp
    parser/ast.cpp
    parser/binder.cpp
)

target_include_directories(entropy_parser
    PUBLIC
        $<BUILD_INTERFACE:${ENTROPY_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(entropy_parser
    PUBLIC
        entropy_common
        entropy_catalog
)

# ─────────────────────────────────────────────────────────────────────────────
# Optimizer Library
# ─────────────────────────────────────────────────────────────────────────────

add_library(entropy_optimizer STATIC
    optimizer/optimizer.cpp
    optimizer/plan_node.cpp
    optimizer/cost_model.cpp
    optimizer/statistics.cpp
    optimizer/index_selector.cpp
)

target_include_directories(entropy_optimizer
    PUBLIC
        $<BUILD_INTERFACE:${ENTROPY_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(entropy_optimizer
    PUBLIC
        entropy_common
        entropy_catalog
        entropy_parser
)

# ─────────────────────────────────────────────────────────────────────────────
# Execution Library
# ─────────────────────────────────────────────────────────────────────────────

add_library(entropy_execution STATIC
    execution/executor.cpp
    execution/seq_scan_executor.cpp
    execution/index_scan_executor.cpp
    execution/insert_executor.cpp
    execution/update_executor.cpp
    execution/delete_executor.cpp
    execution/nested_loop_join.cpp
    execution/hash_join.cpp
    execution/aggregation.cpp
    execution/projection.cpp
    execution/filter.cpp
    execution/sort_executor.cpp
    execution/limit_executor.cpp
)

target_include_directories(entropy_execution
    PUBLIC
        $<BUILD_INTERFACE:${ENTROPY_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(entropy_execution
    PUBLIC
        entropy_common
        entropy_storage
        entropy_catalog
        entropy_transaction
        entropy_optimizer
        entropy_parser
)

# ─────────────────────────────────────────────────────────────────────────────
# Main Library (combines all components)
# ─────────────────────────────────────────────────────────────────────────────

add_library(entropy STATIC
    api/database.cpp
    api/result.cpp
)

target_include_directories(entropy
    PUBLIC
        $<BUILD_INTERFACE:${ENTROPY_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${ENTROPY_GENERATED_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

target_link_libraries(entropy
    PUBLIC
        entropy_common
        entropy_storage
        entropy_catalog
        entropy_transaction
        entropy_parser
        entropy_optimizer
        entropy_execution
)

# ─────────────────────────────────────────────────────────────────────────────
# SQL Shell Executable
# ─────────────────────────────────────────────────────────────────────────────

add_library(entropy_shell_utils STATIC
    api/shell_utils.cpp
)

target_include_directories(entropy_shell_utils
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

add_executable(entropy_shell
    api/shell.cpp
)

target_link_libraries(entropy_shell
    PRIVATE
        entropy
        entropy_shell_utils
)

set_target_properties(entropy_shell PROPERTIES
    OUTPUT_NAME "entropy"
)

install(TARGETS entropy_shell
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# ─────────────────────────────────────────────────────────────────────────────
# Library Install & Export
# ─────────────────────────────────────────────────────────────────────────────

# Every library target reachable from the public `entropy` umbrella is installed
# and added to the `entropyTargets` export set consumed by entropyConfig.cmake.
# entropy_shell_utils is intentionally excluded: it only backs the CLI executable
# and is not part of the public library surface.
install(TARGETS
        entropy
        entropy_common
        entropy_storage
        entropy_catalog
        entropy_transaction
        entropy_parser
        entropy_optimizer
        entropy_execution
    EXPORT entropyTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
