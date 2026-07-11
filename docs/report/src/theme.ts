// Design tokens for the Entropy technical report.
// The palette is anchored to the engine's terminal UI: near-black charcoal,
// amber primary, with cyan/green/red reserved for data semantics.

export const C = {
  // Surfaces
  PAGE: "#0A0A0C",
  PANEL: "#111114",
  PANEL_RAISED: "#17171B",
  WELL: "#060607",

  // Ink
  INK: "#E9E6DE",
  INK_MUTED: "rgba(233, 230, 222, 0.64)",
  INK_SUBTLE: "rgba(233, 230, 222, 0.40)",
  INK_FAINT: "rgba(233, 230, 222, 0.22)",

  // Rules
  HAIRLINE: "rgba(233, 230, 222, 0.14)",
  HAIRLINE_STRONG: "rgba(233, 230, 222, 0.30)",

  // Accents
  AMBER: "#E5A50A",
  AMBER_BRIGHT: "#F2BC45",
  AMBER_DEEP: "#8A6206",
  AMBER_TINT: "rgba(229, 165, 10, 0.10)",
  AMBER_RING: "rgba(229, 165, 10, 0.28)",

  CYAN: "#5FB8C9",
  CYAN_TINT: "rgba(95, 184, 201, 0.12)",
  GREEN: "#58C275",
  GREEN_TINT: "rgba(88, 194, 117, 0.12)",
  RED: "#E0604F",
  RED_TINT: "rgba(224, 96, 79, 0.12)",
} as const;

export const F = {
  SERIF: '"Instrument Serif", Georgia, "Times New Roman", serif',
  SANS: '"Schibsted Grotesk", "Helvetica Neue", Arial, sans-serif',
  MONO: '"IBM Plex Mono", ui-monospace, "SF Mono", Menlo, monospace',
} as const;

// Page geometry (CSS px at 96 dpi; page is 816 x 1056)
export const G = {
  PAGE_W: 816,
  PAGE_H: 1056,
  MARGIN_X: 68,
  MARGIN_TOP: 64,
  MARGIN_BOTTOM: 92,
  FOOTER_Y: 44, // distance of the footer baseline block from the page bottom
} as const;

// Type scale (px). Sizes are tuned for 2x-DPR print through Chromium.
export const T = {
  body: { fontFamily: F.SANS, fontSize: 10.4, lineHeight: 1.56, fontWeight: 400, color: C.INK_MUTED },
  bodyStrong: { fontFamily: F.SANS, fontSize: 10.4, lineHeight: 1.56, fontWeight: 600, color: C.INK },
  lede: { fontFamily: F.SANS, fontSize: 12.5, lineHeight: 1.6, fontWeight: 400, color: C.INK },
  eyebrow: {
    fontFamily: F.MONO,
    fontSize: 8,
    lineHeight: 1.4,
    fontWeight: 500,
    letterSpacing: "0.18em",
    textTransform: "uppercase" as const,
    color: C.AMBER,
  },
  chapterTitle: { fontFamily: F.SERIF, fontSize: 34, lineHeight: 1.12, fontWeight: 400, color: C.INK },
  h2: { fontFamily: F.SANS, fontSize: 13.5, lineHeight: 1.3, fontWeight: 700, color: C.INK },
  h3: { fontFamily: F.SANS, fontSize: 10.8, lineHeight: 1.35, fontWeight: 700, color: C.INK },
  caption: { fontFamily: F.MONO, fontSize: 7.8, lineHeight: 1.55, fontWeight: 400, color: C.INK_SUBTLE },
  mono: { fontFamily: F.MONO, fontSize: 9.2, lineHeight: 1.6, fontWeight: 400 },
  monoSmall: { fontFamily: F.MONO, fontSize: 8.2, lineHeight: 1.55, fontWeight: 400 },
  folio: {
    fontFamily: F.MONO,
    fontSize: 7.6,
    fontWeight: 400,
    letterSpacing: "0.12em",
    textTransform: "uppercase" as const,
    color: C.INK_SUBTLE,
  },
} as const;
