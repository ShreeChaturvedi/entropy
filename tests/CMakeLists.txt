# ─────────────────────────────────────────────────────────────────────────────
# Entropy Database Engine - Test Configuration
# ─────────────────────────────────────────────────────────────────────────────

include(GoogleTest)

# Common test utilities
add_library(test_utils INTERFACE)
target_include_directories(test_utils INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/common)

# ─────────────────────────────────────────────────────────────────────────────
# Storage Tests
# ─────────────────────────────────────────────────────────────────────────────

add_executable(storage_tests
    storage/page_test.cpp
    storage/disk_manager_test.cpp
    storage/buffer_pool_test.cpp
    storage/lru_replacer_test.cpp
    storage/table_page_test.cpp
    storage/tuple_test.cpp
    storage/table_heap_test.cpp
    storage/b_plus_tree_page_test.cpp
    storage/b_plus_tree_test.cpp
    storage/hash_index_test.cpp
)

target_link_libraries(storage_tests
    PRIVATE
        entropy_storage
        entropy_catalog
        test_utils
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(storage_tests)

# ─────────────────────────────────────────────────────────────────────────────
# Catalog Tests
# ─────────────────────────────────────────────────────────────────────────────

add_executable(catalog_tests
    catalog/catalog_test.cpp
)

target_link_libraries(catalog_tests
    PRIVATE
        entropy_catalog
        test_utils
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(catalog_tests)

# ─────────────────────────────────────────────────────────────────────────────
# Transaction Tests
# ─────────────────────────────────────────────────────────────────────────────

add_executable(transaction_tests
    transaction/transaction_test.cpp
)

target_link_libraries(transaction_tests
    PRIVATE
        entropy_transaction
        test_utils
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(transaction_tests)

# ─────────────────────────────────────────────────────────────────────────────
# Integration Tests
# ─────────────────────────────────────────────────────────────────────────────

add_executable(integration_tests
    integration/database_test.cpp
)

target_link_libraries(integration_tests
    PRIVATE
        entropy
        test_utils
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(integration_tests)

# ─────────────────────────────────────────────────────────────────────────────
# Parser Tests
# ─────────────────────────────────────────────────────────────────────────────

add_executable(parser_tests
    parser/parser_test.cpp
)

target_link_libraries(parser_tests
    PRIVATE
        entropy_parser
        entropy_storage
        test_utils
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(parser_tests)

# ─────────────────────────────────────────────────────────────────────────────
# Execution Tests
# ─────────────────────────────────────────────────────────────────────────────

add_executable(execution_tests
    execution/executor_test.cpp
)

target_link_libraries(execution_tests
    PRIVATE
        entropy_execution
        entropy_parser
        entropy_storage
        test_utils
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(execution_tests)

# ─────────────────────────────────────────────────────────────────────────────
# Optimizer Tests
# ─────────────────────────────────────────────────────────────────────────────

add_executable(optimizer_tests
    optimizer/optimizer_test.cpp
)

target_link_libraries(optimizer_tests
    PRIVATE
        entropy_optimizer
        entropy_execution
        entropy_parser
        entropy_catalog
        entropy_storage
        test_utils
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(optimizer_tests)

