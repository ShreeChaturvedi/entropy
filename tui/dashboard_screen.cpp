#include "dashboard_screen.hpp"

#include <string>

#include <ftxui/component/component.hpp>
#include <ftxui/dom/elements.hpp>

#include "primitives.hpp"
#include "theme.hpp"

namespace entropy::tui {

using namespace ftxui;

Component MakeDashboardScreen(const DataSet &data) {
  // Placeholder body. The downstream dashboard replaces this with the KPI
  // strip, invariant/seed matrix, recovery timeline, run table, and fault feed,
  // all built from the primitives. Real aggregates are surfaced here so the
  // data seam is visibly wired.
  return Renderer([data] {
    Element summary = hbox({
        text(std::to_string(data.total) + " runs") | color(theme::kInk1()),
        text("   ") ,
        text(std::to_string(data.passed) + " pass") | color(theme::kGreen()),
        text("   "),
        text(std::to_string(data.failed) + " fail") | color(theme::kRed()),
    });

    Element body = vbox({
        filler(),
        hbox({filler(), text("Crash-simulation dashboard") | bold |
                            color(theme::kInk0()),
              filler()}),
        hbox({filler(), text("under construction") | color(theme::kInk2()),
              filler()}),
        text(""),
        hbox({filler(), summary, filler()}),
        filler(),
    });

    return Panel("CRASH SIMULATOR", std::move(body), /*focused=*/true) |
           theme::canvas_bg();
  });
}

}  // namespace entropy::tui
