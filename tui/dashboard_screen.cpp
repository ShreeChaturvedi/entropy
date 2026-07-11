#include "dashboard_screen.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include <ftxui/component/component.hpp>
#include <ftxui/component/event.hpp>
#include <ftxui/dom/elements.hpp>

#include "primitives.hpp"
#include "theme.hpp"

namespace entropy::tui {

using namespace ftxui;

namespace {

// ── Formatting helpers ───────────────────────────────────────────────────────

/// Left-pad @p s to @p w with spaces (right-aligned numbers).
[[nodiscard]] std::string padl(std::string s, size_t w) {
  if (s.size() < w) {
    s.insert(0, w - s.size(), ' ');
  }
  return s;
}

/// Right-pad @p s to @p w with spaces (left-aligned labels).
[[nodiscard]] std::string padr(std::string s, size_t w) {
  if (s.size() < w) {
    s.append(w - s.size(), ' ');
  }
  return s;
}

/// A seed as a fixed-width hex token, echoing the "0x…" instrument look.
[[nodiscard]] std::string hex_seed(uint64_t seed) {
  char buf[24];
  std::snprintf(buf, sizeof(buf), "0x%04llX",
                static_cast<unsigned long long>(seed));
  return buf;
}

/// A recovery time in milliseconds, or an em dash when the run had no timing.
[[nodiscard]] std::string fmt_ms(const RunRecord &r) {
  if (!r.has_timing) {
    return "—";
  }
  char buf[24];
  std::snprintf(buf, sizeof(buf), "%.2f", r.recovery_ms);
  return buf;
}

[[nodiscard]] std::string fmt2(double v) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%.2f", v);
  return buf;
}

/// A compact schedule name for the tight table columns.
[[nodiscard]] std::string sched_abbrev(const std::string &name) {
  if (name == "mixed") return "mixed";
  if (name == "durable_survives_intact") return "durable";
  if (name == "torn_page_write") return "torn-page";
  if (name == "torn_wal_tail") return "torn-wal";
  if (name == "undo_durable_loser") return "undo-loser";
  return name.substr(0, 10);
}

/// A short crash-point token for the feed lines.
[[nodiscard]] std::string crash_short(const std::string &crash) {
  if (crash == "committed_pages_torn_unsynced") return "torn:page";
  if (crash == "in_flight_txn_wal_tail_torn") return "torn:wal";
  if (crash == "durable_loser_records_then_crash") return "steal:loser";
  if (crash == "mixed_steal_lost_inflight") return "mixed:steal";
  if (crash == "clean_fsynced_baseline") return "clean:base";
  return crash.substr(0, 11);
}

/// A one-line description of the injected fault, for the detail pane.
[[nodiscard]] std::string crash_desc(const std::string &crash) {
  if (crash == "committed_pages_torn_unsynced")
    return "torn page write — committed pages left unsynced at crash";
  if (crash == "in_flight_txn_wal_tail_torn")
    return "torn WAL tail — an in-flight txn's log bytes partially survived";
  if (crash == "durable_loser_records_then_crash")
    return "steal — an aborted loser's records reached durable storage";
  if (crash == "mixed_steal_lost_inflight")
    return "mixed — stolen dirty pages plus a lost in-flight WAL tail";
  if (crash == "clean_fsynced_baseline")
    return "clean baseline — image fully fsynced, no damage applied";
  return crash;
}

/// A run's tri-encoded status. A clean-baseline pass reads as PASS (green); a
/// pass that survived injected faults reads as RECOVERED (cyan); a violated
/// invariant is a FAIL (red).
[[nodiscard]] theme::Status run_status(const RunRecord &r) {
  if (r.outcome == "fail") {
    return theme::Status::kFail;
  }
  if (r.outcome != "pass") {
    return theme::Status::kNeutral;
  }
  return r.faults_injected == 0 ? theme::Status::kPass
                                : theme::Status::kRecovered;
}

// ── Dashboard model (derived once from the dataset) ──────────────────────────

/// One row of the invariant/seed matrix: a schedule and its pass/fail split.
struct ScheduleTile {
  std::string name;
  int total = 0;
  int pass = 0;
  int fail = 0;
};

/// Width of a matrix ratio bar, in cells.
constexpr int kBarCells = 26;

struct Model {
  std::vector<int> order;            ///< run indices in table/feed display order
  std::vector<ScheduleTile> matrix;  ///< one tile row per schedule
  std::string schedules_label;       ///< e.g. "mixed +4"
  std::string seed_range;            ///< e.g. "1..200"
};

/// Build the derived model: a fails-first run ordering and the per-schedule
/// seed heatmap. Pure function of the dataset, so a still is deterministic.
[[nodiscard]] Model BuildModel(const DataSet &data) {
  Model m;

  // Table/feed order: failures first (surface the bugs), then by the schedule's
  // first-seen order, then by seed.
  const auto sched_index = [&](const std::string &name) {
    const auto it =
        std::find(data.schedules.begin(), data.schedules.end(), name);
    return static_cast<int>(it - data.schedules.begin());
  };
  m.order.reserve(data.runs.size());
  for (int i = 0; i < static_cast<int>(data.runs.size()); ++i) {
    m.order.push_back(i);
  }
  std::stable_sort(m.order.begin(), m.order.end(), [&](int a, int b) {
    const RunRecord &ra = data.runs[static_cast<size_t>(a)];
    const RunRecord &rb = data.runs[static_cast<size_t>(b)];
    const int fa = ra.outcome == "fail" ? 0 : 1;
    const int fb = rb.outcome == "fail" ? 0 : 1;
    if (fa != fb) return fa < fb;
    const int sa = sched_index(ra.schedule);
    const int sb = sched_index(rb.schedule);
    if (sa != sb) return sa < sb;
    return ra.seed < rb.seed;
  });

  // Matrix: one tile row per schedule, seeds distributed across fixed buckets so
  // a partially-failing schedule shows a red/green mix.
  for (const std::string &name : data.schedules) {
    ScheduleTile tile;
    tile.name = name;
    std::vector<const RunRecord *> rows;
    for (const RunRecord &r : data.runs) {
      if (r.schedule == name) {
        rows.push_back(&r);
      }
    }
    tile.total = static_cast<int>(rows.size());
    for (const RunRecord *r : rows) {
      if (r->outcome == "fail") {
        ++tile.fail;
      } else {
        ++tile.pass;
      }
    }
    m.matrix.push_back(std::move(tile));
  }

  // Meta strings for the header.
  if (!data.schedules.empty()) {
    m.schedules_label = data.schedules.front();
    if (data.schedules.size() > 1) {
      m.schedules_label +=
          " +" + std::to_string(data.schedules.size() - 1);
    }
  }
  if (!data.runs.empty()) {
    uint64_t lo = data.runs.front().seed;
    uint64_t hi = lo;
    for (const RunRecord &r : data.runs) {
      lo = std::min(lo, r.seed);
      hi = std::max(hi, r.seed);
    }
    m.seed_range = std::to_string(lo) + ".." + std::to_string(hi);
  }
  return m;
}

// ── Header strip ─────────────────────────────────────────────────────────────

[[nodiscard]] Element HeaderStrip(const DataSet &data, const Model &model) {
  const std::string runs = std::to_string(data.total);
  Element title = vbox({
      hbox({
          text("ENTROPY") | bold | color(theme::kInk0()),
          text("  crash-recovery simulator") | color(theme::kInk2()),
      }),
      hbox({
          text(model.schedules_label) | color(theme::kInk1()),
          text("  ·  ") | color(theme::kInk3()),
          text("seeds " + model.seed_range) | color(theme::kInk2()),
          text("  ·  ") | color(theme::kInk3()),
          text("run " + runs + " / " + runs) | color(theme::kInk2()),
      }),
      filler(),
  });

  return hbox({
             GalaxyMark(12, 4),
             text("  "),
             title | flex,
         }) |
         ftxui::size(HEIGHT, EQUAL, 4);
}

// ── KPI strip ────────────────────────────────────────────────────────────────

/// One KPI framed in a thin rounded (unfocused) border.
[[nodiscard]] Element KpiCard(const std::string &label, const std::string &value,
                              const std::string &unit, theme::Status status) {
  return KpiStat(label, value, unit, status) | flex |
         borderStyled(ROUNDED, theme::kInk3()) | bgcolor(theme::kPanel());
}

[[nodiscard]] Element KpiStrip(const DataSet &data) {
  const int distinct = static_cast<int>(data.distinct_invariants.size());
  return hbox({
             KpiCard("SEEDS PASSED", std::to_string(data.passed),
                     "/ " + std::to_string(data.total), theme::Status::kPass) |
                 flex,
             KpiCard("INVARIANTS", std::to_string(distinct),
                     distinct == 0 ? "clean" : "failed",
                     distinct == 0 ? theme::Status::kPass
                                   : theme::Status::kFail) |
                 flex,
             KpiCard("RECOVERY p99", fmt2(data.recovery_p99), "ms",
                     theme::Status::kRecovered) |
                 flex,
             KpiCard("FAULTS FIRED", std::to_string(data.total_faults), "",
                     theme::Status::kNeutral) |
                 flex,
         }) |
         ftxui::size(HEIGHT, EQUAL, 6);
}

// ── Recovery sweep bar ───────────────────────────────────────────────────────

[[nodiscard]] Element SweepPanel(const DataSet &data) {
  const double rate = data.pass_rate();
  Element bar = gauge(static_cast<float>(rate)) | color(theme::kCyan()) |
                bgcolor(theme::kPanelHi()) | flex;

  Element body = vbox({
      filler(),
      hbox({
          text("run " + std::to_string(data.total) + " / " +
               std::to_string(data.total) + "  ") |
              color(theme::kInk2()),
          bar,
          text("   recovered ") | color(theme::kInk2()),
          text(std::to_string(data.passed)) | color(theme::kGreen()),
          text("  ·  FAIL ") | color(theme::kInk2()),
          text(std::to_string(data.failed)) | color(theme::kRed()),
      }),
      filler(),
  });

  char pct[16];
  std::snprintf(pct, sizeof(pct), "%.1f%% recovered", rate * 100.0);
  return Panel("RECOVERY SWEEP", std::move(body), /*focused=*/false, pct) |
         ftxui::size(HEIGHT, EQUAL, 5);
}

// ── Invariant / seed matrix ──────────────────────────────────────────────────

[[nodiscard]] Element MatrixPanel(const Model &model) {
  Elements rows;
  for (const ScheduleTile &tile : model.matrix) {
    // A stacked pass/fail ratio bar: green for the recovered share, red for the
    // violated share. Any failure keeps at least one red cell visible.
    int red = tile.total == 0 ? 0
                              : static_cast<int>(std::lround(
                                    static_cast<double>(tile.fail) * kBarCells /
                                    tile.total));
    if (tile.fail > 0) {
      red = std::max(red, 1);
    }
    red = std::min(red, kBarCells);
    const int green = kBarCells - red;
    Elements cells;
    for (int c = 0; c < green; ++c) {
      cells.push_back(text("█") | color(theme::kGreen()));
    }
    for (int c = 0; c < red; ++c) {
      cells.push_back(text("█") | color(theme::kRed()));
    }

    Element triple = hbox({
        text("(") | color(theme::kInk3()),
        text(std::to_string(tile.pass)) | color(theme::kInk1()),
        text(":") | color(theme::kInk3()),
        text(std::to_string(tile.total)) | color(theme::kInk2()),
        text(":") | color(theme::kInk3()),
        text(std::to_string(tile.fail)) |
            color(tile.fail > 0 ? theme::kRed() : theme::kInk3()),
        text(")") | color(theme::kInk3()),
    });

    rows.push_back(hbox({
        text(padr(sched_abbrev(tile.name), 11)) | color(theme::kInk1()),
        text(" "),
        hbox(std::move(cells)),
        filler(),
        text(" "),
        std::move(triple),
    }));
  }

  return Panel("INVARIANT / SEED MATRIX", vbox(std::move(rows)) | flex) | flex;
}

// ── Recovery timeline ────────────────────────────────────────────────────────

[[nodiscard]] Element TimelinePanel(const DataSet &data) {
  Element chart =
      TimelineChart("recovery ms", data.recovery_series, 40, 4);
  return Panel("RECOVERY TIMELINE", chart | flex) | flex;
}

// ── Run/feed shared row list ─────────────────────────────────────────────────

/// Render the runs (in display order) as a row-lit, scrollable list: @p sel
/// gets the amber band and neighbors fall off, and the selected row is scrolled
/// into view. @p make_row draws one run's row. Shared by the run table and the
/// fault feed so the lighting/scroll behavior stays identical.
template <typename MakeRow>
[[nodiscard]] Element LitScrollList(const DataSet &data, const Model &model,
                                    int sel, MakeRow make_row) {
  Elements rows;
  rows.reserve(model.order.size());
  for (int i = 0; i < static_cast<int>(model.order.size()); ++i) {
    const RunRecord &r =
        data.runs[static_cast<size_t>(model.order[static_cast<size_t>(i)])];
    Element row = make_row(r) | RowGlow(std::abs(i - sel));
    if (i == sel) {
      row = row | focus;
    }
    rows.push_back(std::move(row));
  }
  return vbox(std::move(rows)) | yframe;
}

// ── Run table ────────────────────────────────────────────────────────────────

constexpr size_t kWSeed = 8, kWSched = 11, kWStatus = 10, kWInv = 4, kWFlt = 5,
                 kWRec = 8;

[[nodiscard]] Element RunTableHeader() {
  return hbox({
             text("   "),
             text(padr("SEED", kWSeed)) | color(theme::kInk3()),
             text(padr("SCHEDULE", kWSched)) | color(theme::kInk3()),
             text(padr("STATUS", kWStatus)) | color(theme::kInk3()),
             text(padl("INV", kWInv)) | color(theme::kInk3()),
             text(padl("FLT", kWFlt)) | color(theme::kInk3()),
             text(padl("REC ms", kWRec)) | color(theme::kInk3()),
             filler(),
         }) |
         bold;
}

[[nodiscard]] Element RunTableRow(const RunRecord &r) {
  const theme::Status st = run_status(r);
  const int inv = static_cast<int>(r.invariants_failed.size());
  return hbox({
      text(" "),
      StatusGlyph(st),
      text(" "),
      text(padr(hex_seed(r.seed), kWSeed)) | color(theme::kInk1()),
      text(padr(sched_abbrev(r.schedule), kWSched)) | color(theme::kInk2()),
      text(padr(theme::status_word(st), kWStatus)) |
          color(theme::status_color(st)),
      text(padl(inv == 0 ? "·" : std::to_string(inv), kWInv)) |
          color(inv == 0 ? theme::kInk3() : theme::kRed()),
      text(padl(std::to_string(r.faults_injected), kWFlt)) |
          color(theme::kInk2()),
      text(padl(fmt_ms(r), kWRec)) | color(theme::kInk1()),
      filler(),
  });
}

[[nodiscard]] Element RunTablePanel(const DataSet &data, const Model &model,
                                    int sel) {
  Element body = vbox({
      RunTableHeader(),
      separatorLight() | color(theme::kInk3()),
      LitScrollList(data, model, sel, RunTableRow) | flex,
  });

  const std::string ann =
      std::to_string(sel + 1) + " / " + std::to_string(model.order.size());
  return Panel("RUN TABLE", std::move(body), /*focused=*/true, ann) | flex;
}

// ── Fault feed (master / detail) ─────────────────────────────────────────────

[[nodiscard]] Element FeedRow(const RunRecord &r) {
  const theme::Status st = run_status(r);
  Elements cols = {
      text(padl(std::to_string(r.seed), 4)) | color(theme::kInk2()),
      text(" "),
      StatusGlyph(st),
      text(" "),
      text(padr(theme::status_word(st), kWStatus)) |
          color(theme::status_color(st)),
      text(padr(crash_short(r.crash_point), 12)) | color(theme::kInk1()),
      text(padl(fmt_ms(r), 7)) | color(theme::kInk2()),
  };
  if (!r.invariants_failed.empty()) {
    cols.push_back(text("  " + r.invariants_failed.front()) |
                   color(theme::kRed()));
  }
  cols.push_back(filler());
  return hbox(std::move(cols));
}

[[nodiscard]] Element FeedDetail(const RunRecord &r) {
  const theme::Status st = run_status(r);

  Element inv_line =
      r.invariants_failed.empty()
          ? hbox({text("invariants  ") | color(theme::kInk2()),
                  text("all held") | color(theme::kGreen())})
          : hbox({text("invariants  ") | color(theme::kInk2()),
                  text(r.invariants_failed.front()) | color(theme::kRed()),
                  text("  violated") | color(theme::kInk2())});

  return vbox({
      hbox({
          text("seed ") | color(theme::kInk2()),
          text(hex_seed(r.seed)) | color(theme::kInk0()),
          text("   ") | color(theme::kInk3()),
          StatusBadge(st),
          filler(),
          text(sched_abbrev(r.schedule)) | color(theme::kInk1()),
      }),
      hbox({
          text("crash  ") | color(theme::kInk2()),
          text(r.crash_point) | color(theme::kInk1()),
      }),
      hbox({
          text("fault  ") | color(theme::kInk2()),
          text(crash_desc(r.crash_point)) | color(theme::kInk1()),
      }),
      hbox({
          text("replay ") | color(theme::kInk2()),
          text("fired " + std::to_string(r.faults_injected)) |
              color(theme::kInk1()),
          text("  ·  redo " + std::to_string(r.redo_ops)) |
              color(theme::kInk2()),
          text("  ·  undo " + std::to_string(r.undo_ops)) |
              color(theme::kInk2()),
          text("  ·  " + fmt_ms(r) + " ms") | color(theme::kInk1()),
      }),
      inv_line,
  });
}

[[nodiscard]] Element FeedPanel(const DataSet &data, const Model &model,
                                int sel) {
  const RunRecord &cur =
      data.runs[static_cast<size_t>(model.order[static_cast<size_t>(sel)])];

  Element body = vbox({
      LitScrollList(data, model, sel, FeedRow) | flex,
      separatorLight() | color(theme::kInk3()),
      FeedDetail(cur),
  });
  return Panel("FAULT FEED", std::move(body)) | flex;
}

// ── Legend bar ───────────────────────────────────────────────────────────────

[[nodiscard]] Element LegendBar() {
  const auto key = [](const std::string &k, const std::string &desc) {
    return hbox({
        text(k) | color(theme::kInk1()),
        text(" " + desc + "   ") | color(theme::kInk3()),
    });
  };
  return hbox({
             key("[↑↓]", "select"),
             key("[⏎]", "inspect"),
             key("[g/G]", "top/end"),
             key("[tab]", "console"),
             key("[esc]", "boot"),
             key("[q]", "quit"),
             filler(),
         }) |
         ftxui::size(HEIGHT, EQUAL, 1);
}

}  // namespace

Component MakeDashboardScreen(const DataSet &data) {
  struct State {
    DataSet data;
    Model model;
    int sel = 0;
  };
  auto state = std::make_shared<State>();
  state->data = data;
  state->model = BuildModel(data);

  const auto view = [state] {
    const DataSet &d = state->data;
    const Model &m = state->model;

    if (m.order.empty()) {
      Element empty =
          vbox({filler(),
                hbox({filler(), text("no runs loaded") | color(theme::kInk2()),
                      filler()}),
                filler()});
      return Panel("CRASH SIMULATOR", std::move(empty), /*focused=*/true) |
             theme::canvas_bg();
    }

    Element mid = hbox({
                      MatrixPanel(m) | flex,
                      TimelinePanel(d) | flex,
                  }) |
                  ftxui::size(HEIGHT, EQUAL, 9);

    Element bottom = hbox({
        RunTablePanel(d, m, state->sel) | flex,
        FeedPanel(d, m, state->sel) | flex,
    });

    return vbox({
               HeaderStrip(d, m),
               KpiStrip(d),
               SweepPanel(d),
               mid,
               bottom | flex,
               LegendBar(),
           }) |
           theme::canvas_bg() | color(theme::kInk1());
  };

  const auto handler = [state](const Event &e) {
    const int n = static_cast<int>(state->model.order.size());
    if (n == 0) {
      return false;
    }
    if (e == Event::ArrowDown || e == Event::Character('j')) {
      state->sel = std::min(state->sel + 1, n - 1);
      return true;
    }
    if (e == Event::ArrowUp || e == Event::Character('k')) {
      state->sel = std::max(state->sel - 1, 0);
      return true;
    }
    if (e == Event::PageDown) {
      state->sel = std::min(state->sel + 10, n - 1);
      return true;
    }
    if (e == Event::PageUp) {
      state->sel = std::max(state->sel - 10, 0);
      return true;
    }
    if (e == Event::Character('g') || e == Event::Home) {
      state->sel = 0;
      return true;
    }
    if (e == Event::Character('G') || e == Event::End) {
      state->sel = n - 1;
      return true;
    }
    return false;
  };

  return CatchEvent(Renderer(view), handler);
}

// ── Deterministic replay demo (for animated capture) ─────────────────────────

namespace {

// Frame budget for the dashboard replay, split into four contiguous phases.
constexpr int kDemoStream = 60;   ///< runs streaming into the table
constexpr int kDemoHold1 = 8;     ///< dwell on the fully-populated board
constexpr int kDemoInspect = 34;  ///< spotlight sweeping back up the table
constexpr int kDemoHold2 = 12;    ///< dwell on the final inspected run
constexpr int kDemoRevealCap = 340;  ///< runs revealed by the end of streaming

/// Full dataset reordered for the stream: by seed, then by each schedule's
/// first-seen position, so every seed's cohort (all schedules, the occasional
/// torn-page failure among them) lands together and the matrix, timeline, and
/// gauge all grow at once instead of one schedule at a time.
[[nodiscard]] std::vector<int> DemoStreamOrder(const DataSet &full) {
  std::vector<int> order(full.runs.size());
  for (int i = 0; i < static_cast<int>(order.size()); ++i) {
    order[static_cast<size_t>(i)] = i;
  }
  const auto sched_index = [&](const std::string &name) {
    const auto it =
        std::find(full.schedules.begin(), full.schedules.end(), name);
    return static_cast<int>(it - full.schedules.begin());
  };
  std::stable_sort(order.begin(), order.end(), [&](int a, int b) {
    const RunRecord &ra = full.runs[static_cast<size_t>(a)];
    const RunRecord &rb = full.runs[static_cast<size_t>(b)];
    if (ra.seed != rb.seed) return ra.seed < rb.seed;
    return sched_index(ra.schedule) < sched_index(rb.schedule);
  });
  return order;
}

}  // namespace

int DashboardDemoFrameCount() {
  return kDemoStream + kDemoHold1 + kDemoInspect + kDemoHold2;
}

Element RenderDashboardDemoFrame(const DataSet &full, int step) {
  const int frames = DashboardDemoFrameCount();
  step = std::clamp(step, 0, frames - 1);

  const int cap = std::min<int>(kDemoRevealCap,
                                static_cast<int>(full.runs.size()));
  const std::vector<int> stream = DemoStreamOrder(full);

  // Resolve this frame's reveal count and selected row from the phase timeline.
  int revealed = cap;
  int sel = cap - 1;
  if (step < kDemoStream) {
    const double frac = static_cast<double>(step + 1) /
                        static_cast<double>(kDemoStream);
    revealed = std::clamp(static_cast<int>(std::lround(cap * frac)), 6, cap);
    sel = revealed - 1;  // spotlight rides the newest landed run
  } else if (step < kDemoStream + kDemoHold1) {
    revealed = cap;
    sel = cap - 1;
  } else if (step < kDemoStream + kDemoHold1 + kDemoInspect) {
    revealed = cap;
    const int k = step - (kDemoStream + kDemoHold1);
    sel = std::clamp(cap - 1 - k, 0, cap - 1);  // sweep the spotlight upward
  } else {
    revealed = cap;
    sel = std::clamp(cap - 1 - kDemoInspect, 0, cap - 1);
  }

  // Build the partial dataset (first `revealed` runs in stream order) and a
  // model whose display order is that same stream order, so rows only ever
  // append at the bottom and the spotlight tails them cleanly.
  DataSet d;
  d.runs.reserve(static_cast<size_t>(revealed));
  for (int i = 0; i < revealed; ++i) {
    d.runs.push_back(full.runs[static_cast<size_t>(stream[static_cast<size_t>(i)])]);
  }
  Recompute(d);

  Model m = BuildModel(d);
  m.order.resize(static_cast<size_t>(revealed));
  for (int i = 0; i < revealed; ++i) {
    m.order[static_cast<size_t>(i)] = i;
  }

  sel = std::clamp(sel, 0, std::max(0, revealed - 1));

  Element mid = hbox({
                    MatrixPanel(m) | flex,
                    TimelinePanel(d) | flex,
                }) |
                ftxui::size(HEIGHT, EQUAL, 9);

  Element bottom = hbox({
      RunTablePanel(d, m, sel) | flex,
      FeedPanel(d, m, sel) | flex,
  });

  return vbox({
             HeaderStrip(d, m),
             KpiStrip(d),
             SweepPanel(d),
             mid,
             bottom | flex,
             LegendBar(),
         }) |
         theme::canvas_bg() | color(theme::kInk1());
}

}  // namespace entropy::tui
