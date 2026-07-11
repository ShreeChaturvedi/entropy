#pragma once

/**
 * @file theme.hpp
 * @brief The single source of truth for the entropy TUI's visual language.
 *
 * Restraint is the whole point. The palette is a near-black charcoal canvas, a
 * four-step neutral text ramp, exactly ONE amber accent reserved for focus, and
 * a tightly bounded set of saturated status colors. No screen may hardcode a
 * color: pull everything from here so the look stays coherent as screens grow.
 *
 * Accent rule: amber marks focus and nothing else (the focused panel border and
 * title, the active table-row band, the selected tab). Never scatter it.
 *
 * Status budget (the only saturated colors allowed): green=pass, red=fail,
 * cyan=recovered, amber=in-flight.
 *
 * The palette is exposed as inline accessor functions rather than namespace-
 * scope Color constants on purpose: an ftxui::Color built at static-init time
 * dereferences FTXUI terminal state that may not be constructed yet (a
 * static-initialization-order fiasco). Function-local statics defer every color
 * to first use, which is always after main() has started.
 */

#include <string>

#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>

namespace entropy::tui::theme {

using ftxui::Color;

// ── Canvas ──────────────────────────────────────────────────────────────────
// Near-black, very slightly warm so amber and the neutrals feel related.
[[nodiscard]] inline Color kCanvas() { return Color::RGB(14, 14, 16); }
[[nodiscard]] inline Color kPanel() { return Color::RGB(20, 20, 23); }
[[nodiscard]] inline Color kPanelHi() { return Color::RGB(28, 28, 32); }

// ── Neutral text ramp (four steps, brightest -> faintest) ────────────────────
[[nodiscard]] inline Color kInk0() { return Color::RGB(232, 231, 224); }
[[nodiscard]] inline Color kInk1() { return Color::RGB(174, 173, 167); }
[[nodiscard]] inline Color kInk2() { return Color::RGB(112, 112, 110); }
[[nodiscard]] inline Color kInk3() { return Color::RGB(66, 67, 71); }

// ── The one accent: amber, for focus only ────────────────────────────────────
[[nodiscard]] inline Color kAmber() { return Color::RGB(255, 176, 0); }
[[nodiscard]] inline Color kAmberDim() { return Color::RGB(92, 64, 8); }
[[nodiscard]] inline Color kAmberDeep() { return Color::RGB(46, 33, 6); }

// ── Status colors (the entire saturated budget) ──────────────────────────────
[[nodiscard]] inline Color kGreen() { return Color::RGB(122, 194, 112); }
[[nodiscard]] inline Color kRed() { return Color::RGB(224, 106, 100); }
[[nodiscard]] inline Color kCyan() { return Color::RGB(92, 184, 198); }

/// The semantic states a status glyph, cell, or badge can encode. Each maps to
/// one color, one glyph, and one word so the encoding is triple-redundant and
/// still legible without color.
enum class Status : uint8_t {
  kPass,
  kFail,
  kRecovered,
  kInFlight,
  kNeutral,
};

/// The lone color for a status. Used for text, glyphs, and cell fills alike.
[[nodiscard]] inline Color status_color(Status s) {
  switch (s) {
    case Status::kPass:
      return kGreen();
    case Status::kFail:
      return kRed();
    case Status::kRecovered:
      return kCyan();
    case Status::kInFlight:
      return kAmber();
    case Status::kNeutral:
      return kInk2();
  }
  return kInk2();
}

/// A filled glyph for a status. Solid marks read cleanly at cell resolution.
[[nodiscard]] inline const char *status_glyph(Status s) {
  switch (s) {
    case Status::kPass:
      return "●";
    case Status::kFail:
      return "◆";
    case Status::kRecovered:
      return "◉";
    case Status::kInFlight:
      return "◐";
    case Status::kNeutral:
      return "○";
  }
  return "○";
}

/// The uppercase word for a status (the third, colorblind-safe encoding).
[[nodiscard]] inline const char *status_word(Status s) {
  switch (s) {
    case Status::kPass:
      return "PASS";
    case Status::kFail:
      return "FAIL";
    case Status::kRecovered:
      return "RECOVERED";
    case Status::kInFlight:
      return "IN-FLIGHT";
    case Status::kNeutral:
      return "IDLE";
  }
  return "IDLE";
}

/// Map a simulator outcome string ("pass" | "fail" | "error") to a status.
[[nodiscard]] inline Status status_from_outcome(const std::string &outcome) {
  if (outcome == "pass") {
    return Status::kPass;
  }
  if (outcome == "fail") {
    return Status::kFail;
  }
  return Status::kNeutral;  // "error" and anything unexpected read as neutral
}

// ── Shared decorators ────────────────────────────────────────────────────────

/// Paint an element with the page's charcoal canvas background.
[[nodiscard]] inline ftxui::Decorator canvas_bg() {
  return ftxui::bgcolor(kCanvas());
}

/// Shorthand for the four neutral ramp steps (0 = brightest, 3 = faintest).
[[nodiscard]] inline Color ink(int level) {
  switch (level) {
    case 0:
      return kInk0();
    case 1:
      return kInk1();
    case 2:
      return kInk2();
    default:
      return kInk3();
  }
}

}  // namespace entropy::tui::theme
