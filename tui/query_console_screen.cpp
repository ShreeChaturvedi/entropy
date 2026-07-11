#include "query_console_screen.hpp"

#include <ftxui/component/component.hpp>
#include <ftxui/dom/elements.hpp>

#include "primitives.hpp"
#include "theme.hpp"

namespace entropy::tui {

using namespace ftxui;

Component MakeQueryConsoleScreen() {
  // Placeholder body. The downstream console replaces this with the schema
  // sidebar, Query/History/Plan tabs, the SQL editor, and the typed result
  // grid, driving entropy::Database::execute over libentropy.
  return Renderer([] {
    Element body = vbox({
        filler(),
        hbox({filler(), text("SQL query console") | bold | color(theme::kInk0()),
              filler()}),
        hbox({filler(), text("under construction") | color(theme::kInk2()),
              filler()}),
        text(""),
        hbox({filler(),
              text("entropy> ") | color(theme::kInk2()),
              text("SELECT * FROM runs;") | color(theme::kInk3()),
              filler()}),
        filler(),
    });

    return Panel("QUERY CONSOLE", std::move(body), /*focused=*/false) |
           theme::canvas_bg();
  });
}

}  // namespace entropy::tui
