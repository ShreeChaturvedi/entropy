#include "primitives.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <utility>

#include <ftxui/dom/canvas.hpp>

#include "theme.hpp"

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

/// Dark-mode sheen top: near-white. Light-mode sheen top: cool mid ink so the
/// specular still reads without washing out on paper.
constexpr Rgb kWhite{247.0, 248.0, 252.0};
constexpr Rgb kLightSheen{72.0, 74.0, 82.0};

/// Linear interpolation between two RGB triples, returning the triple.
[[nodiscard]] Rgb lerp(const Rgb &a, const Rgb &b, double t) {
  const double u = std::clamp(t, 0.0, 1.0);
  return {a.r + (b.r - a.r) * u, a.g + (b.g - a.g) * u, a.b + (b.b - a.b) * u};
}

/// Pack an RGB triple into an ftxui::Color, clamping each channel to a byte.
[[nodiscard]] Color to_color(const Rgb &c) {
  return Color::RGB(chan(c.r), chan(c.g), chan(c.b));
}

/// Linear interpolation between two RGB triples, collapsed to an ftxui::Color.
[[nodiscard]] Color mix(const Rgb &a, const Rgb &b, double t) {
  return to_color(lerp(a, b, t));
}

/// Smoothstep easing for gentler ramps.
[[nodiscard]] double smoothstep(double t) {
  const double u = std::clamp(t, 0.0, 1.0);
  return u * u * (3.0 - 2.0 * u);
}

/// An unnormalized Gaussian exp(-(x-mu)^2 / 2 sigma^2), peak 1 at x == mu.
[[nodiscard]] double gaussian(double x, double mu, double sigma) {
  const double d = x - mu;
  return std::exp(-(d * d) / (2.0 * sigma * sigma));
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

/// A 4x4 Bayer ordered-dither threshold in (0,1). Gives the galaxy a regular
/// halftone dot texture (reads as a designed dot-matrix, not random speckle).
[[nodiscard]] double bayer(int x, int y) {
  static constexpr int kM[4][4] = {
      {0, 8, 2, 10}, {12, 4, 14, 6}, {3, 11, 1, 9}, {15, 7, 13, 5}};
  return (static_cast<double>(kM[y & 3][x & 3]) + 0.5) / 16.0;
}

/// The galaxy brightness ramp. Same structure keys in both modes; luminance
/// direction flips so the mark stays a metallic disc on charcoal and an ink
/// disc on paper.
[[nodiscard]] Rgb galaxy_ramp(double lum) {
  if (theme::is_light()) {
    // Structural intensity → ink depth: faint rim -> graphite -> near-black core.
    const Rgb edge{186.0, 186.0, 190.0};
    const Rgb steel{110.0, 112.0, 120.0};
    const Rgb deep{48.0, 50.0, 56.0};
    const Rgb core{22.0, 22.0, 26.0};
    if (lum < 0.34) {
      return lerp(edge, steel, smoothstep(lum / 0.34));
    }
    if (lum < 0.70) {
      return lerp(steel, deep, smoothstep((lum - 0.34) / 0.36));
    }
    return lerp(deep, core, smoothstep((lum - 0.70) / 0.30));
  }
  // Dark: charcoal rim -> steel -> silver -> white core.
  const Rgb edge{34.0, 36.0, 44.0};
  const Rgb steel{116.0, 122.0, 138.0};
  const Rgb silver{196.0, 200.0, 210.0};
  if (lum < 0.34) {
    return lerp(edge, steel, smoothstep(lum / 0.34));
  }
  if (lum < 0.70) {
    return lerp(steel, silver, smoothstep((lum - 0.34) / 0.36));
  }
  return lerp(silver, kWhite, smoothstep((lum - 0.70) / 0.30));
}

[[nodiscard]] Rgb sheen_top() {
  return theme::is_light() ? kLightSheen : kWhite;
}

}  // namespace

// ── (a) Galaxy dot-matrix mark ───────────────────────────────────────────────

Element GalaxyMark(int cell_cols, int cell_rows, double sweep_phase) {
  const int w = std::max(cell_cols, 2) * 2;   // braille sub-pixels wide
  const int h = std::max(cell_rows, 2) * 4;   // braille sub-pixels tall
  Canvas canvas(w, h);

  const double cx = static_cast<double>(w) * 0.5;
  const double cy = static_cast<double>(h) * 0.5;
  // Isotropic normalization (divide both axes by the shorter half-extent) so a
  // circle in disc space stays a circle regardless of the cell aspect ratio.
  const double norm = 1.0 / std::min(cx, cy);

  // Disc inclination: rotate the major axis onto a diagonal, then compress the
  // minor axis so the mark reads as a tilted disc rather than a flat circle.
  constexpr double kTilt = 0.50;  // radians
  const double ct = std::cos(kTilt);
  const double st = std::sin(kTilt);
  constexpr double kInclination = 1.72;  // minor-axis compression

  // Static baked luster: a fixed diagonal sheen so the still reads as brushed
  // metal. The animated sweep (only when sweep_phase >= 0) rides on top of it.
  constexpr double kBandCenter = -0.05;
  constexpr double kBandWidth = 0.33;

  // Animated sweep: a narrow bright band travelling along the same diagonal. At
  // phase 0 and phase 1 it sits fully off-disc, so a phase 0->1 capture loops
  // seamlessly; mid-cycle it crosses the bright core.
  const bool animate = sweep_phase >= 0.0;
  const double sweep_center = -1.7 + 3.4 * std::clamp(sweep_phase, 0.0, 1.0);
  constexpr double kSweepWidth = 0.17;

  // Point-field geometry (see the per-pixel field below): a mid-radius torus
  // ring and two log-spiral arms r = a*exp(b*theta) that peel off it. A bright
  // ring pushed out to r ~ 0.56 leaves a dark elliptical void between it and the
  // tight nucleus, and the arms trace two clearly-separated curves off the ring.
  constexpr double kPi = 3.14159265358979323846;
  constexpr double kRingRadius = 0.56;  // torus radius; opens a dark inner hole
  constexpr double kArmRef = 0.16;      // a: radius where the arm crosses 0
  constexpr double kArmPitch = 0.70;    // b: larger = looser winding
  constexpr double kArmWidth = 0.070;   // perpendicular half-width of an arm

  for (int y = 0; y < h; ++y) {
    for (int x = 0; x < w; ++x) {
      const double fx = (static_cast<double>(x) - cx) * norm;
      const double fy = (static_cast<double>(y) - cy) * norm;

      // Rotate into disc space, then compress the minor axis for the tilt.
      const double dx = fx * ct - fy * st;
      const double dy = (fx * st + fy * ct) * kInclination;
      const double r = std::sqrt(dx * dx + dy * dy);
      if (r > 1.14) {
        continue;
      }
      const double theta = std::atan2(dy, dx);

      // Luminance field built from INTENTIONAL structure, not dithered scatter:
      // a tight bright nucleus with a compact bulge hugging it, a mid-radius
      // torus ring that opens a dark elliptical void, and two logarithmic-spiral
      // arms traced deliberately out of that ring. The background stays
      // near-empty so the geometry reads at this low resolution, and everything
      // is kept off pure white to leave headroom for the diagonal sheen.

      // (1) A tight, very sharp nucleus so only the very center saturates to
      // white (the mark's single brightest point), with a compact silver bulge
      // hugging it. High weight and small sigma give the core its punch.
      const double core = 1.45 * gaussian(r, 0.0, 0.068);
      const double bulge = 0.12 * gaussian(r, 0.0, 0.085);

      // (2) The torus ring at mid-radius, so a real dark elliptical hole opens
      // between it and the nucleus (that void, not more dots, is what makes the
      // mark read as a spiral disc). Thin and bright so it lights near-continuous
      // around the ellipse, but kept below the nucleus so the core still wins.
      const double ring = 0.80 * gaussian(r, kRingRadius, 0.040);

      // (3) Two log-spiral arms that peel off the ring. The spiral angle at
      // radius r is phi(r) = ln(r/a)/b; the arms sit at phi and phi+pi. Fold
      // theta onto the nearest arm, measure the (chord) perpendicular distance,
      // and light a narrow band along it. A smoothstep inner edge makes the arms
      // grow out of the ring instead of filling the hole, and they dim outward.
      double arm_lum = 0.0;
      if (r > 0.52) {  // arms start at the ring shoulder (where `inner` opens up)
        const double phi = std::log(r / kArmRef) / kArmPitch;
        double d = theta - phi;
        d -= kPi * std::round(d / kPi);  // fold onto nearest of the two arms
        const double perp = r * d;       // ~perpendicular distance to the arm
        const double inner = smoothstep((r - 0.52) / 0.10);  // grow from ring
        const double outward = std::exp(-r / 0.82);          // dim outward
        arm_lum = 1.05 * gaussian(perp, 0.0, kArmWidth) * inner * outward;
      }

      // (4) Near-empty background: only a whisper of halo so the void between
      // the arms is not dead black.
      const double halo = 0.015 * std::exp(-r / 0.5);

      double base = core + bulge + ring + arm_lum + halo;

      // Diagonal projection shared by the static luster and the moving sweep,
      // with a radial presence mask so both fade with the disc.
      const double diag = (fx - fy) * 0.70710678;
      const double presence = std::exp(-r / 0.62);

      // Broad directional shading: the disc catches light along the diagonal so
      // one flank sits brighter than the other, reading as a lit metallic
      // surface rather than a flat radial blob.
      const double lit = 0.72 + 0.38 * smoothstep((0.34 - diag) / 1.05);
      double lum = base * lit;

      // Narrow specular sheen: a bright diagonal streak baked static (survives
      // the still) plus, when animating, a brighter travelling sweep on top.
      const double luster = gaussian(diag, kBandCenter, kBandWidth) * presence;
      lum += 0.40 * luster;

      double sweep = 0.0;
      if (animate) {
        sweep = gaussian(diag, sweep_center, kSweepWidth) * presence;
        lum += 0.58 * sweep;
      }
      lum = std::clamp(lum, 0.0, 1.0);

      // Density tracks luminance: the near-white core lights every sub-pixel (a
      // solid mass) while the rim dissolves into sparse dots. A halftone dither
      // keeps the dot-matrix texture without the field reading as random noise.
      const double thresh = 0.90 * bayer(x, y) + 0.06 * hash01(x, y);
      if (lum * 1.06 <= thresh) {
        continue;
      }

      // Mode-aware ramp, lifted toward the sheen top under the static band and
      // (when present) the moving sweep.
      Rgb rgb = galaxy_ramp(lum);
      const double shine = std::clamp(luster * 0.85 + sweep, 0.0, 1.0);
      if (shine > 0.12) {
        rgb = lerp(rgb, sheen_top(), smoothstep((shine - 0.12) / 0.55) * 0.8);
      }
      canvas.DrawPoint(x, y, true, to_color(rgb));
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
    return bgcolor(theme::kAmberDim());  // the spotlight row: the one accent
  }
  // btop-style spotlight falloff: the background fades progressively from a dim
  // warm-neutral glow just below the amber peak down to the bare canvas over
  // several rows, so the selection reads as a smooth gradient, not a lone band.
  constexpr int kFalloff = 6;
  if (distance >= kFalloff) {
    return bgcolor(theme::kCanvas());
  }
  const double bright =
      static_cast<double>(kFalloff - distance) / static_cast<double>(kFalloff);
  // Endpoints mirror theme::kCanvas / a soft lift so light mode never paints
  // charcoal ghosts under list rows. Keep numbers in lockstep with theme.hpp.
  const Rgb base =
      theme::is_light() ? Rgb{250.0, 249.0, 246.0} : Rgb{14.0, 14.0, 16.0};
  const Rgb glow =
      theme::is_light() ? Rgb{240.0, 239.0, 235.0} : Rgb{52.0, 49.0, 44.0};
  return bgcolor(mix(base, glow, smoothstep(bright)));
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
