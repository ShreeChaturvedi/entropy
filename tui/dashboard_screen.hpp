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

#include "data_loader.hpp"

namespace entropy::tui {

/// Build the dashboard screen over @p data. Owns its own state.
[[nodiscard]] ftxui::Component MakeDashboardScreen(const DataSet &data);

}  // namespace entropy::tui
