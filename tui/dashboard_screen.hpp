#pragma once

/**
 * @file dashboard_screen.hpp
 * @brief Factory for the crash-simulation dashboard screen.
 *
 * STUB: returns a labeled placeholder that compiles and renders on the shared
 * substrate. The full dashboard (KPI strip, invariant/seed matrix, recovery
 * timeline, run table, fault feed) is built on top of the primitives here.
 */

#include <ftxui/component/component.hpp>
#include <ftxui/dom/elements.hpp>

#include "data_loader.hpp"

namespace entropy::tui {

/// Build the dashboard screen over @p data. Owns its own state.
[[nodiscard]] ftxui::Component MakeDashboardScreen(const DataSet &data);

/// Total frame count of the dashboard replay demo (see RenderDashboardDemoFrame).
[[nodiscard]] int DashboardDemoFrameCount();

/// Render one frame of a deterministic dashboard replay: runs stream into the
/// table seed-by-seed (KPIs, matrix, timeline, and gauge climbing with them),
/// then the row spotlight sweeps back up the settled table so the fault feed
/// steps through individual crash/recovery outcomes. @p full is the complete
/// dataset; @p step in [0, DashboardDemoFrameCount()) selects the frame. Pure
/// function of (@p full, @p step), so every frame is reproducible.
[[nodiscard]] ftxui::Element RenderDashboardDemoFrame(const DataSet &full,
                                                      int step);

}  // namespace entropy::tui
