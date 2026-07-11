#include "boot_screen.hpp"

#include <cstdio>

#include "primitives.hpp"
#include "theme.hpp"

namespace entropy::tui {

using namespace ftxui;

const std::vector<BootMenuItem> &BootMenu() {
  static const std::vector<BootMenuItem> kItems = {
      {"Crash Simulator", "1"},
      {"Query Console", "2"},
      {"Recovery Report", "3"},
      {"Quit", "q"},
  };
  return kItems;
}

namespace {

/// The letter-spaced ENTROPY wordmark, in the brightest ink.
[[nodiscard]] Element Wordmark() {
  return text("E N T R O P Y") | bold | color(theme::kInk0());
}

/// The one-line stats footer, Grok-style: a hairline rule then muted facts.
[[nodiscard]] Element StatsFooter(const DataSet &data,
                                  const std::string &version) {
  char rate[16];
  std::snprintf(rate, sizeof(rate), "%.1f%%", data.pass_rate() * 100.0);
  char p99[24];
  std::snprintf(p99, sizeof(p99), "%.2f ms", data.recovery_p99);

  const std::string runs = std::to_string(data.total) + " runs";
  const std::string sched = std::to_string(data.schedules.size()) + " schedules";

  Element dot = text("  ·  ") | color(theme::kInk3());
  return hbox({
      text("─ ") | color(theme::kInk3()),
      text("entropy " + version) | color(theme::kInk2()),
      dot,
      text(runs) | color(theme::kInk2()),
      dot,
      text(sched) | color(theme::kInk2()),
      dot,
      text(std::string(rate) + " pass") | color(theme::kInk2()),
      dot,
      text("recovery p99 " + std::string(p99)) | color(theme::kInk2()),
      filler(),
      text("truecolor ") | color(theme::kInk3()),
  });
}

[[nodiscard]] Element Menu(int selected_index) {
  const std::vector<BootMenuItem> &items = BootMenu();
  Elements rows;
  rows.reserve(items.size());
  for (int i = 0; i < static_cast<int>(items.size()); ++i) {
    const bool on = (i == selected_index);
    const BootMenuItem &item = items[static_cast<size_t>(i)];
    Element marker = text(on ? "› " : "  ") | color(theme::kAmber());
    Element label = on ? (text(item.label) | bold | color(theme::kAmber()))
                       : (text(item.label) | color(theme::kInk1()));
    Element key = on ? (text(item.key) | color(theme::kAmber()))
                     : (text(item.key) | color(theme::kInk3()));
    rows.push_back(hbox({
        marker,
        label,
        filler(),
        text("  "),
        key,
    }));
  }
  return vbox(std::move(rows));
}

}  // namespace

Element BootScreen(const DataSet &data, int selected_index,
                   const std::string &version, double galaxy_phase) {
  Element mark = GalaxyMark(26, 13, galaxy_phase);

  // The text column is pinned to a fixed width so the right-aligned menu keys
  // land inside a stable column and never ride out onto the panel border (the
  // clip that broke the legend into stray vertical bars near 120 cols). The
  // longest line ("Replayable fault injection ...") fits comfortably here.
  constexpr int kTextWidth = 64;
  Element right = vbox({
                      hbox({Wordmark(), text("   "),
                            text("v" + version) | color(theme::kInk2())}),
                      text(""),
                      text("Deterministic crash-recovery simulator") |
                          color(theme::kInk1()),
                      text("Replayable fault injection · WAL redo/undo · "
                           "invariant oracle") |
                          color(theme::kInk2()),
                      text(""),
                      text(""),
                      Menu(selected_index),
                  }) |
                  ftxui::size(WIDTH, EQUAL, kTextWidth);

  Element card = hbox({
      mark,
      text("   "),
      std::move(right),
  });

  // A blank line and a two-space gutter on every side keep the content off the
  // rounded border so nothing clips at the frame, at 120 cols or wider.
  Element bordered = vbox({
                         text(""),
                         hbox({text("  "), std::move(card), text("  ")}),
                         text(""),
                     }) |
                     borderStyled(ROUNDED, theme::kInk3()) |
                     bgcolor(theme::kPanel());

  // Center the card in the page, footer pinned to the bottom.
  return vbox({
             filler(),
             hbox({filler(), std::move(bordered), filler()}),
             filler(),
             StatsFooter(data, version),
         }) |
         theme::canvas_bg() | color(theme::kInk1());
}

}  // namespace entropy::tui
