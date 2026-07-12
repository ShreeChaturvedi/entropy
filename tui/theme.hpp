#pragma once

/**
 * @file theme.hpp
 * @brief The single source of truth for the entropy TUI's visual language.
 *
 * Restraint is the whole point. The palette is a near-black charcoal canvas (or
 * its light-mode mirror: warm paper), a four-step neutral text ramp, exactly
 * ONE amber accent reserved for focus, and a tightly bounded set of saturated
 * status colors. No screen may hardcode a color: pull everything from here so
 * the look stays coherent as screens grow.
 *
 * Two modes share the same rules and roles; only the luminance direction flips:
 *   dark  — light ink on charcoal
 *   light — dark ink on warm paper
 * Amber, status colors, and the four-step ramp keep the same jobs in both.
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

#include <cstdint>
#include <string>

#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>

namespace entropy::tui::theme {

using ftxui::Color;

/// Visual mode. Capture scripts and a future interactive toggle share this.
enum class Mode : uint8_t { kDark = 0, kLight = 1 };

/// Process-wide mode. Capture sets it before rendering; the interactive app
/// defaults to dark.
[[nodiscard]] inline Mode &mode_ref() {
  static Mode m = Mode::kDark;
  return m;
}

inline void set_mode(Mode m) { mode_ref() = m; }

[[nodiscard]] inline Mode mode() { return mode_ref(); }

[[nodiscard]] inline bool is_light() { return mode_ref() == Mode::kLight; }

[[nodiscard]] inline bool is_dark() { return mode_ref() == Mode::kDark; }

// ── Canvas ──────────────────────────────────────────────────────────────────
// Dark: near-black, very slightly warm so amber and the neutrals feel related.
// Light: warm paper, the luminance mirror of that charcoal.
[[nodiscard]] inline Color kCanvas() {
  return is_light() ? Color::RGB(250, 249, 246) : Color::RGB(14, 14, 16);
}
[[nodiscard]] inline Color kPanel() {
  return is_light() ? Color::RGB(255, 255, 253) : Color::RGB(20, 20, 23);
}
[[nodiscard]] inline Color kPanelHi() {
  return is_light() ? Color::RGB(240, 239, 235) : Color::RGB(28, 28, 32);
}

// ── Neutral text ramp (four steps, strongest -> faintest) ────────────────────
// Dark: bright -> dim. Light: near-black -> mid gray. Paper needs a steeper
// ramp than charcoal: mid grays that looked fine as "dim" on black wash out
// on white, so light-mode steps stay darker overall.
[[nodiscard]] inline Color kInk0() {
  return is_light() ? Color::RGB(12, 12, 14) : Color::RGB(232, 231, 224);
}
[[nodiscard]] inline Color kInk1() {
  return is_light() ? Color::RGB(36, 36, 40) : Color::RGB(174, 173, 167);
}
[[nodiscard]] inline Color kInk2() {
  return is_light() ? Color::RGB(68, 68, 74) : Color::RGB(112, 112, 110);
}
[[nodiscard]] inline Color kInk3() {
  return is_light() ? Color::RGB(120, 120, 126) : Color::RGB(66, 67, 71);
}

// ── The one accent: amber, for focus only ────────────────────────────────────
// Same hue in both modes. Dim/deep flip role: dark mode uses deep shadows;
// light mode uses soft washes under selected chrome.
[[nodiscard]] inline Color kAmber() { return Color::RGB(255, 176, 0); }
[[nodiscard]] inline Color kAmberDim() {
  return is_light() ? Color::RGB(255, 220, 150) : Color::RGB(92, 64, 8);
}
[[nodiscard]] inline Color kAmberDeep() {
  return is_light() ? Color::RGB(255, 236, 200) : Color::RGB(46, 33, 6);
}

// ── Status colors (the entire saturated budget) ──────────────────────────────
// Slightly deeper on light so they hold contrast on paper.
[[nodiscard]] inline Color kGreen() {
  return is_light() ? Color::RGB(56, 148, 72) : Color::RGB(122, 194, 112);
}
[[nodiscard]] inline Color kRed() {
  return is_light() ? Color::RGB(200, 80, 74) : Color::RGB(224, 106, 100);
}
[[nodiscard]] inline Color kCyan() {
  return is_light() ? Color::RGB(40, 150, 165) : Color::RGB(92, 184, 198);
}

/// Hex canvas for capture tooling (freeze/Chrome page background), no '#'.
[[nodiscard]] inline const char *canvas_hex_digits() {
  return is_light() ? "faf9f6" : "0e0e10";
}

/// Hex canvas with leading '#', for freeze --background.
[[nodiscard]] inline const char *canvas_hex() {
  return is_light() ? "#faf9f6" : "#0e0e10";
}

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

/// Paint an element with the page's canvas background.
[[nodiscard]] inline ftxui::Decorator canvas_bg() {
  return ftxui::bgcolor(kCanvas());
}

/// Shorthand for the four neutral ramp steps (0 = strongest, 3 = faintest).
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
