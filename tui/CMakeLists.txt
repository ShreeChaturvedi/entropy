# ─────────────────────────────────────────────────────────────────────────────
# Entropy Terminal UI
#
# Built only when ENTROPY_BUILD_TUI=ON. FTXUI is vendored via FetchContent so
# the whole UI stays a pure C++ / CMake story with no system packages.
# ─────────────────────────────────────────────────────────────────────────────

include(FetchContent)

# Keep the fetched build lean: we only need the libraries, not FTXUI's own
# docs / examples / tests.
set(FTXUI_BUILD_DOCS OFF CACHE BOOL "" FORCE)
set(FTXUI_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(FTXUI_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(FTXUI_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)

FetchContent_Declare(ftxui
    GIT_REPOSITORY https://github.com/ArthurSonzogni/FTXUI
    GIT_TAG v7.0.0
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(ftxui)

# FTXUI is a vendored third-party dependency. The engine's strict warning set
# (added tree-wide via add_compile_options, including -Werror under CI) is our
# contract, not upstream's, so silence warnings on the fetched targets. This
# does NOT relax warnings on the entropy-tui sources below.
foreach(_ftxui_target screen dom component)
    if(TARGET ${_ftxui_target})
        target_compile_options(${_ftxui_target} PRIVATE -w)
    endif()
endforeach()

# Sources are listed explicitly (no globbing) so downstream work edits these
# files rather than silently adding new translation units.
add_executable(entropy-tui
    main.cpp
    data_loader.cpp
    primitives.cpp
    boot_screen.cpp
    app_shell.cpp
    dashboard_screen.cpp
    query_console_screen.cpp
)

target_include_directories(entropy-tui
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# ftxui::component -> dom -> screen (transitive), plus the entropy engine for
# the query console's Database API.
target_link_libraries(entropy-tui
    PRIVATE
        ftxui::component
        ftxui::dom
        ftxui::screen
        entropy
)

# Resolve the bundled demo data relative to the source tree so a dev build can
# run `entropy-tui` from anywhere and still find tui/data/*.jsonl.
target_compile_definitions(entropy-tui
    PRIVATE
        ENTROPY_TUI_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/data"
)
