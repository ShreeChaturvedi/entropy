#pragma once

/**
 * @file primitives.hpp
 * @brief Reusable renderers that carry the entropy TUI's visual language.
 *
 * Every screen is assembled from these. They own the look (the galaxy mark, the
 * rounded focus panels, the instrument-style KPIs, the status encoding, the
 * row-lighting falloff, the braille timeline) so no screen re-invents it and the
 * amber-on-charcoal restraint holds everywhere.
 *
 * All effects are baked static: the galaxy's luster band and the row glow both
 * survive a single captured frame. Nothing here depends on catching an
 * animation frame.
 */

#include <string>
#include <vector>

#include <ftxui/dom/elements.hpp>

#include "theme.hpp"

namespace entropy::tui {

using ftxui::Decorator;
using ftxui::Element;
using ftxui::Elements;

// ── (a) Galaxy dot-matrix mark ───────────────────────────────────────────────

/**
 * The hero mark: a braille dot-matrix spiral galaxy whose cells are filled with
 * a per-point 24-bit ramp from a silver-white core out to charcoal edges, with
 * a STATIC diagonal luster band baked in for a metallic sheen.
 *
 * @param cell_cols  width in terminal character cells (each is 2 braille dots)
 * @param cell_rows  height in terminal character cells (each is 4 braille dots)
 *
 * Deterministic: the same size always renders byte-identical output, so it is
 * safe for still capture.
 */
[[nodiscard]] Element GalaxyMark(int cell_cols, int cell_rows);

// ── (b) Rounded focus panel ──────────────────────────────────────────────────

/**
 * A thin rounded-border panel with a title. When @p focused, the border and
 * title turn amber (the accent's only job); otherwise both sit in the neutral
 * ramp. @p annotation renders right-aligned in the title row (e.g. "(200 rows)").
 */
[[nodiscard]] Element Panel(const std::string &title, Element body,
                            bool focused = false,
                            const std::string &annotation = "");

// ── (c) Instrument KPI singlestat ────────────────────────────────────────────

/**
 * A dot-matrix seven-segment KPI readout: a dim label, big segmented digits for
 * @p value, and a trailing @p unit. The digits and unit take the status color,
 * so threshold state is encoded in both the number's color and (by the caller's
 * choice of status) its meaning.
 */
[[nodiscard]] Element KpiStat(const std::string &label, const std::string &value,
                              const std::string &unit, theme::Status status);

// ── (d) Tri-encoded status ───────────────────────────────────────────────────

/// Just the colored status glyph (glyph + color).
[[nodiscard]] Element StatusGlyph(theme::Status status);

/// Glyph + colored word: the full triple encoding (glyph, color, word).
[[nodiscard]] Element StatusBadge(theme::Status status);

// ── (e) Row lighting ─────────────────────────────────────────────────────────

/**
 * btop-style row lighting as a background decorator. @p distance is the row's
 * offset from the active row: 0 is the active row (a full-width amber band),
 * and larger values fall off toward the canvas so neighbors glow dimmer. Apply
 * to a full-width row element.
 */
[[nodiscard]] Decorator RowGlow(int distance);

/**
 * Convenience: stack pre-built full-width @p rows into a lit list where
 * @p active_index gets the brightest band and neighbors fall off. Rows keep
 * their own foreground content; only the background is lit.
 */
[[nodiscard]] Element RowLitList(const Elements &rows, int active_index);

// ── (f) Braille timeline chart ───────────────────────────────────────────────

/**
 * A braille line chart of @p series with a header line reading min / p99 / max.
 * The line is drawn with a vertical brightness ramp (taller samples read
 * brighter silver) and a faint p99 guide line. Empty @p series renders an
 * "awaiting samples" placeholder at the requested size.
 *
 * @param cell_cols  plot width in character cells
 * @param cell_rows  plot height in character cells (excludes the header row)
 */
[[nodiscard]] Element TimelineChart(const std::string &title,
                                    const std::vector<double> &series,
                                    int cell_cols, int cell_rows);

/// The p-th percentile (0..100) of @p values by linear interpolation. Returns 0
/// for an empty input. Exposed because KPI panels and the chart share it.
[[nodiscard]] double Percentile(std::vector<double> values, double p);

}  // namespace entropy::tui
