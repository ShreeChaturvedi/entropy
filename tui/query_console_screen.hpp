#pragma once

/**
 * @file query_console_screen.hpp
 * @brief Factory for the SQL query console screen.
 *
 * STUB: returns a labeled placeholder that compiles and renders on the shared
 * substrate. The full console (schema sidebar, tab strip, line-numbered editor
 * with restrained SQL highlighting, typed result grid) links libentropy and
 * drives Database::execute; that is built on top of the primitives here.
 */

#include <ftxui/component/component.hpp>

namespace entropy::tui {

/// Build the query console screen. Owns its own state.
[[nodiscard]] ftxui::Component MakeQueryConsoleScreen();

}  // namespace entropy::tui
