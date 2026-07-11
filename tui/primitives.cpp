#include "primitives.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <utility>

#include <ftxui/dom/canvas.hpp>

namespace entropy::tui {

using namespace ftxui;  // Color, Canvas, and the dom element builders

namespace {

// ── Small color helpers ──────────────────────────────────────────────────────

/// Clamp a double to a byte channel.
[[nodiscard]] uint8_t chan(double v) {
  return static_cast<uint8_t>(std::lround(std::clamp(v, 0.0, 255.0)));
}

struct Rgb {
  double r, g, b;
};

/// Linear interpolation between two RGB triples.
[[nodiscard]] Color mix(const Rgb &a, const Rgb &b, double t) {
  const double u = std::clamp(t, 0.0, 1.0);
  return Color::RGB(chan(a.r + (b.r - a.r) * u), chan(a.g + (b.g - a.g) * u),
                    chan(a.b + (b.b - a.b) * u));
}

/// Smoothstep easing for gentler ramps.
[[nodiscard]] double smoothstep(double t) {
  const double u = std::clamp(t, 0.0, 1.0);
  return u * u * (3.0 - 2.0 * u);
}

/// Deterministic spatial hash in [0,1). Used to dither the galaxy dot matrix so
/// the ramp reads as texture rather than smooth bands.
[[nodiscard]] double hash01(int x, int y) {
  uint32_t h = static_cast<uint32_t>(x) * 0x9E3779B1u ^
               static_cast<uint32_t>(y) * 0x85EBCA77u;
  h ^= h >> 15;
  h *= 0x2C1B3C6Du;
  h ^= h >> 12;
  return static_cast<double>(h & 0xFFFFFFu) / static_cast<double>(0x1000000u);
}

}  // namespace

// ── (a) Galaxy dot-matrix mark ───────────────────────────────────────────────

Element GalaxyMark(int cell_cols, int cell_rows) {
  const int w = std::max(cell_cols, 2) * 2;   // braille sub-pixels wide
  const int h = std::max(cell_rows, 2) * 4;   // braille sub-pixels tall
  Canvas canvas(w, h);

  const double cx = static_cast<double>(w) * 0.5;
  const double cy = static_cast<double>(h) * 0.5;
  const double inv_rx = 1.0 / cx;
  const double inv_ry = 1.0 / cy;

  // Silver-white core -> steel mid -> charcoal edge.
  const Rgb core{240.0, 241.0, 246.0};
  const Rgb steel{146.0, 152.0, 166.0};
  const Rgb edge{44.0, 46.0, 54.0};

  // The disc is inclined (one axis compressed) and rotated so its major axis
  // runs on a diagonal, echoing the reference blackhole's tilt.
  constexpr double kTilt = 0.42;  // radians
  const double ct = std::cos(kTilt);
  const double st = std::sin(kTilt);

  for (int y = 0; y < h; ++y) {
    for (int x = 0; x < w; ++x) {
      const double fx = (static_cast<double>(x) - cx) * inv_rx;
      const double fy = (static_cast<double>(y) - cy) * inv_ry;
      // Rotate into disc space, then compress the minor axis for inclination.
      const double rx = fx * ct - fy * st;
      const double ry = (fx * st + fy * ct) * 1.7;
      const double r = std::sqrt(rx * rx + ry * ry);
      if (r > 1.08) {
        continue;
      }

      const double angle = std::atan2(ry, rx);
      // Two logarithmic spiral arms modulate a smooth disc.
      const double arm =
          0.5 + 0.5 * std::cos(2.0 * angle - 6.5 * std::log(r + 0.10));
      const double arm_sharp = std::pow(arm, 3.0);
      const double disc = std::exp(-r * 2.2);             // overall falloff
      const double coreglow = std::exp(-(r * r) * 26.0);  // tight nucleus

      double bright = coreglow * 0.95 + disc * (0.18 + 0.82 * arm_sharp);

      // Baked static diagonal luster band: a fixed bright streak so the mark
      // reads as brushed metal in a still. This is NOT animated here.
      const double diag = (fx - fy) * 0.5;
      const double luster = std::exp(-(diag + 0.15) * (diag + 0.15) * 5.0);
      bright += luster * disc * 0.5;
      // Cap below 1 so even the nucleus keeps a little dot-matrix breakup
      // instead of flooding to a solid white patch.
      bright = std::clamp(bright, 0.0, 0.93);

      // Full-range ordered dither: the probability a cell lights scales with its
      // brightness, so even the bright disc keeps a dot-matrix texture instead
      // of flooding to solid white.
      if (bright < hash01(x, y) * 0.96 + 0.02) {
        continue;
      }

      // Ramp: charcoal -> steel -> silver, then lift toward white in the band.
      Color col = bright < 0.5
                      ? mix(edge, steel, smoothstep(bright / 0.5))
                      : mix(steel, core, smoothstep((bright - 0.5) / 0.5));
      const double shine = luster * disc;
      if (shine > 0.18) {
        col = Color::RGB(chan(232 + shine * 23.0), chan(234 + shine * 21.0),
                         chan(240 + shine * 15.0));
      }

      canvas.DrawPoint(x, y, true, col);
    }
  }

  return ftxui::canvas(std::move(canvas));
}

// ── (b) Rounded focus panel ──────────────────────────────────────────────────

Element Panel(const std::string &title, Element body, bool focused,
              const std::string &annotation) {
  const Color border_col = focused ? theme::kAmber() : theme::kInk3();
  const Color title_col = focused ? theme::kAmber() : theme::kInk1();

  Element title_el = text(" " + title + " ") | bold | color(title_col);
  Element header =
      annotation.empty()
          ? title_el
          : hbox({title_el, filler(),
                  text(annotation + " ") | color(theme::kInk2())});

  Element framed = vbox({
                       header,
                       separatorLight() | color(theme::kInk3()),
                       std::move(body) | flex,
                   }) |
                   flex;

  return framed | borderStyled(ROUNDED, border_col) |
         bgcolor(focused ? theme::kPanelHi() : theme::kPanel());
}

// ── (c) Instrument KPI singlestat ────────────────────────────────────────────

namespace {

/// Three 3-wide rows of a compact seven-segment glyph for one character.
[[nodiscard]] std::array<std::string, 3> seg_glyph(char c) {
  switch (c) {
    case '0': return {" _ ", "| |", "|_|"};
    case '1': return {"   ", "  |", "  |"};
    case '2': return {" _ ", " _|", "|_ "};
    case '3': return {" _ ", " _|", " _|"};
    case '4': return {"   ", "|_|", "  |"};
    case '5': return {" _ ", "|_ ", " _|"};
    case '6': return {" _ ", "|_ ", "|_|"};
    case '7': return {" _ ", "  |", "  |"};
    case '8': return {" _ ", "|_|", "|_|"};
    case '9': return {" _ ", "|_|", " _|"};
    case '-': return {"   ", " _ ", "   "};
    case '.': return {"   ", "   ", " . "};
    case '/': return {"  /", " / ", "/  "};
    case ':': return {"   ", " . ", " . "};
    default:  return {"   ", " " + std::string(1, c) + " ", "   "};
  }
}

}  // namespace

Element KpiStat(const std::string &label, const std::string &value,
                const std::string &unit, theme::Status status) {
  std::array<std::string, 3> rows{"", "", ""};
  for (char c : value) {
    const std::array<std::string, 3> g = seg_glyph(c);
    for (int i = 0; i < 3; ++i) {
      rows[static_cast<size_t>(i)] += g[static_cast<size_t>(i)] + " ";
    }
  }

  const Color val_col = theme::status_color(status);
  Element digits = vbox({
                       text(rows[0]),
                       text(rows[1]),
                       text(rows[2]),
                   }) |
                   color(val_col);

  Element unit_el =
      unit.empty() ? filler()
                   : vbox({filler(), text(unit) | color(theme::kInk2())});

  Element readout = unit.empty()
                        ? digits
                        : hbox({digits, text(" "), std::move(unit_el)});

  return vbox({
      text(label) | color(theme::kInk2()),
      std::move(readout),
  });
}

// ── (d) Tri-encoded status ───────────────────────────────────────────────────

Element StatusGlyph(theme::Status status) {
  return text(theme::status_glyph(status)) | color(theme::status_color(status));
}

Element StatusBadge(theme::Status status) {
  const Color col = theme::status_color(status);
  return hbox({
      text(theme::status_glyph(status)) | color(col),
      text(" "),
      text(theme::status_word(status)) | color(col),
  });
}

// ── (e) Row lighting ─────────────────────────────────────────────────────────

Decorator RowGlow(int distance) {
  if (distance <= 0) {
    return bgcolor(theme::kAmberDim());  // the active-row band (the one accent)
  }
  constexpr int kFalloff = 4;
  const double frac =
      static_cast<double>(std::min(distance, kFalloff)) / kFalloff;
  const Rgb near{28.0, 28.0, 32.0};  // kPanelHi
  const Rgb far{14.0, 14.0, 16.0};   // kCanvas
  return bgcolor(mix(near, far, frac));
}

Element RowLitList(const Elements &rows, int active_index) {
  Elements lit;
  lit.reserve(rows.size());
  for (int i = 0; i < static_cast<int>(rows.size()); ++i) {
    lit.push_back(rows[static_cast<size_t>(i)] |
                  RowGlow(std::abs(i - active_index)));
  }
  return vbox(std::move(lit));
}

// ── (f) Braille timeline chart ───────────────────────────────────────────────

double Percentile(std::vector<double> values, double p) {
  if (values.empty()) {
    return 0.0;
  }
  std::sort(values.begin(), values.end());
  if (values.size() == 1) {
    return values.front();
  }
  const double rank =
      std::clamp(p, 0.0, 100.0) / 100.0 * static_cast<double>(values.size() - 1);
  const auto lo = static_cast<size_t>(std::floor(rank));
  const auto hi = static_cast<size_t>(std::ceil(rank));
  const double frac = rank - static_cast<double>(lo);
  return values[lo] + (values[hi] - values[lo]) * frac;
}

namespace {

[[nodiscard]] std::string fmt2(double v) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%.2f", v);
  return std::string(buf);
}

}  // namespace

Element TimelineChart(const std::string &title,
                      const std::vector<double> &series, int cell_cols,
                      int cell_rows) {
  const double vmin = series.empty()
                          ? 0.0
                          : *std::min_element(series.begin(), series.end());
  const double vmax = series.empty()
                          ? 0.0
                          : *std::max_element(series.begin(), series.end());
  const double p99 = Percentile(series, 99.0);

  Element header = hbox({
      text(title) | bold | color(theme::kInk1()),
      filler(),
      text("min ") | color(theme::kInk2()),
      text(fmt2(vmin)) | color(theme::kInk0()),
      text("  p99 ") | color(theme::kInk2()),
      text(fmt2(p99)) | color(theme::kInk0()),
      text("  max ") | color(theme::kInk2()),
      text(fmt2(vmax)) | color(theme::kInk0()),
  });

  const int w = std::max(cell_cols, 2) * 2;
  const int h = std::max(cell_rows, 2) * 4;

  if (series.empty()) {
    return vbox({
        header,
        vbox({filler(),
              hbox({filler(), text("awaiting samples") | color(theme::kInk3()),
                    filler()}),
              filler()}) |
            ftxui::size(HEIGHT, EQUAL, cell_rows),
    });
  }

  Canvas canvas(w, h);
  const double span = (vmax > vmin) ? (vmax - vmin) : 1.0;
  const double p99_frac = std::clamp((p99 - vmin) / span, 0.0, 1.0);
  const int p99_y =
      (h - 1) - static_cast<int>(std::lround(p99_frac * static_cast<double>(h - 1)));

  // Faint p99 guide line.
  for (int gx = 0; gx < w; gx += 2) {
    canvas.DrawPoint(gx, p99_y, true, theme::kInk3());
  }

  const auto n = static_cast<int>(series.size());
  const auto y_of = [&](int i) {
    const double frac = std::clamp((series[static_cast<size_t>(i)] - vmin) / span,
                                   0.0, 1.0);
    return (h - 1) - static_cast<int>(std::lround(frac * static_cast<double>(h - 1)));
  };
  const auto x_of = [&](int i) {
    if (n == 1) {
      return 0;
    }
    return static_cast<int>(std::lround(static_cast<double>(i) *
                                        static_cast<double>(w - 1) /
                                        static_cast<double>(n - 1)));
  };

  const Rgb dim{70.0, 74.0, 84.0};
  const Rgb bright{224.0, 228.0, 236.0};
  for (int i = 1; i < n; ++i) {
    const int x1 = x_of(i - 1);
    const int y1 = y_of(i - 1);
    const int x2 = x_of(i);
    const int y2 = y_of(i);
    // Brightness ramps with the taller endpoint: peaks read as bright silver.
    const double frac =
        std::clamp((static_cast<double>((h - 1) - std::min(y1, y2)) /
                    static_cast<double>(h - 1)),
                   0.0, 1.0);
    canvas.DrawPointLine(x1, y1, x2, y2, mix(dim, bright, frac));
  }

  return vbox({header, ftxui::canvas(std::move(canvas))});
}

}  // namespace entropy::tui
