import type { ReactNode } from "react";
import { Page } from "./Page";
import { C, F, G } from "../theme";

/** Deterministic PRNG (mulberry32) so the divider dot fields never shift between builds. */
export function mulberry32(seed: number) {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Sparse dot field, echoing the TUI's braille-galaxy boot screen. */
export function DotField({
  seed,
  width,
  height,
  count,
  amberShare = 0.06,
}: {
  seed: number;
  width: number;
  height: number;
  count: number;
  amberShare?: number;
}) {
  const rnd = mulberry32(seed);
  const dots = [];
  for (let i = 0; i < count; i++) {
    // Cluster into a loose diagonal band with uniform scatter behind it.
    const band = rnd() < 0.72;
    const t = rnd();
    const x = band ? t * width : rnd() * width;
    const spine = height * (0.82 - 0.55 * t);
    const y = band ? spine + (rnd() - 0.5) * height * 0.34 * (0.4 + rnd()) : rnd() * height;
    const r = rnd() < 0.85 ? 0.9 : 1.7;
    const isAmber = rnd() < amberShare;
    const o = 0.12 + rnd() * (isAmber ? 0.75 : 0.38);
    dots.push(
      <circle key={i} cx={x} cy={y} r={r} fill={isAmber ? C.AMBER : C.INK} opacity={o} />,
    );
  }
  return <g>{dots}</g>;
}

export function PartDivider({
  numeral,
  title,
  seed,
  items,
}: {
  numeral: string;
  title: ReactNode;
  seed: number;
  items: { n: string; label: string }[];
}) {
  return (
    <Page chrome={false}>
      <svg
        width={G.PAGE_W}
        height={G.PAGE_H}
        viewBox={`0 0 ${G.PAGE_W} ${G.PAGE_H}`}
        style={{ position: "absolute", inset: 0 }}
      >
        <rect width={G.PAGE_W} height={G.PAGE_H} fill={C.PAGE} />
        <DotField seed={seed} width={G.PAGE_W} height={G.PAGE_H} count={520} />
      </svg>
      <div
        style={{
          position: "absolute",
          inset: `0 ${G.MARGIN_X}px`,
          display: "flex",
          flexDirection: "column",
          justifyContent: "center",
        }}
      >
        <div
          style={{
            fontFamily: F.MONO,
            fontSize: 8.5,
            letterSpacing: "0.22em",
            textTransform: "uppercase",
            color: C.AMBER,
            marginBottom: 18,
          }}
        >
          Part {numeral}
        </div>
        <div
          style={{
            fontFamily: F.SERIF,
            fontSize: 58,
            lineHeight: 1.06,
            color: C.INK,
            maxWidth: 520,
            marginBottom: 40,
          }}
        >
          {title}
        </div>
        <div style={{ borderTop: `0.5px solid ${C.HAIRLINE_STRONG}`, maxWidth: 520 }}>
          {items.map((item) => (
            <div
              key={item.n}
              style={{
                display: "flex",
                gap: 18,
                alignItems: "baseline",
                padding: "10px 0",
                borderBottom: `0.5px solid ${C.HAIRLINE}`,
              }}
            >
              <span style={{ fontFamily: F.MONO, fontSize: 8.5, color: C.AMBER }}>{item.n}</span>
              <span style={{ fontFamily: F.SANS, fontSize: 12, fontWeight: 500, color: C.INK_MUTED }}>
                {item.label}
              </span>
            </div>
          ))}
        </div>
      </div>
    </Page>
  );
}
