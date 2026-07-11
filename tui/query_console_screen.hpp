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
#include <ftxui/dom/elements.hpp>

namespace entropy::tui {

/// Build the query console screen. Owns its own state.
[[nodiscard]] ftxui::Component MakeQueryConsoleScreen();

/// Total frame count of the console replay demo (see RenderConsoleDemoFrame).
[[nodiscard]] int ConsoleDemoFrameCount();

/// Render one frame of a deterministic console session: a fresh database is
/// built by typing CREATE TABLE and INSERTs, then queried with a filtered
/// SELECT and an ORDER BY / LIMIT, with the result grid, history, and query
/// plan shown against the live engine. @p step in [0, ConsoleDemoFrameCount())
/// selects the frame; every statement runs for real, so the grids are genuine
/// engine output. Pure function of @p step (each frame replays from scratch).
[[nodiscard]] ftxui::Element RenderConsoleDemoFrame(int step);

}  // namespace entropy::tui
