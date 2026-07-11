import { Page } from "../components/Page";
import { mulberry32 } from "../components/Divider";
import { C, F, G } from "../theme";

/** Generative spiral galaxy, echoing the TUI's braille boot mark. */
function Galaxy({ cx, cy, scale, seed }: { cx: number; cy: number; scale: number; seed: number }) {
  const rnd = mulberry32(seed);
  const dots = [];
  // Two arms of a logarithmic spiral with jitter, plus a scatter halo.
  for (let arm = 0; arm < 2; arm++) {
    const phase = arm * Math.PI;
    for (let i = 0; i < 340; i++) {
      const t = i / 340;
      const theta = phase + t * 4.6 * Math.PI;
      const r = scale * (0.055 + 0.95 * Math.pow(t, 0.78));
      const jr = (rnd() - 0.5) * scale * 0.16 * (0.25 + t);
      const x = cx + (r + jr) * Math.cos(theta) * 1.18;
      const y = cy + (r + jr) * Math.sin(theta) * 0.72;
      const isAmber = rnd() < 0.045;
      const o = (1 - t * 0.6) * (0.1 + rnd() * (isAmber ? 0.8 : 0.42));
      const rad = rnd() < 0.82 ? 1.0 : 1.9;
      dots.push(<circle key={`${arm}-${i}`} cx={x} cy={y} r={rad} fill={isAmber ? C.AMBER : C.INK} opacity={o} />);
    }
  }
  for (let i = 0; i < 130; i++) {
    const a = rnd() * Math.PI * 2;
    const r = scale * (0.2 + rnd() * 1.35);
    dots.push(
      <circle
        key={`h-${i}`}
        cx={cx + r * Math.cos(a) * 1.2}
        cy={cy + r * Math.sin(a) * 0.75}
        r={0.8}
        fill={C.INK}
        opacity={0.05 + rnd() * 0.16}
      />,
    );
  }
  return <g>{dots}</g>;
}

export function Cover() {
  return (
    <Page chrome={false}>
      <svg
        width={G.PAGE_W}
        height={G.PAGE_H}
        viewBox={`0 0 ${G.PAGE_W} ${G.PAGE_H}`}
        style={{ position: "absolute", inset: 0 }}
      >
        <rect width={G.PAGE_W} height={G.PAGE_H} fill={C.PAGE} />
        <Galaxy cx={545} cy={330} scale={215} seed={0x51} />
      </svg>

      {/* top meta */}
      <div
        style={{
          position: "absolute",
          top: 56,
          left: 64,
          right: 64,
          display: "flex",
          justifyContent: "space-between",
          borderTop: `1px solid ${C.HAIRLINE_STRONG}`,
          paddingTop: 12,
          fontFamily: F.MONO,
          fontSize: 8,
          letterSpacing: "0.18em",
          textTransform: "uppercase",
          color: C.INK_SUBTLE,
        }}
      >
        <span>Technical report</span>
        <span>July 2026 · v0.1.1</span>
      </div>

      {/* title block */}
      <div style={{ position: "absolute", left: 64, right: 64, bottom: 148 }}>
        <div
          style={{
            fontFamily: F.MONO,
            fontSize: 15,
            fontWeight: 600,
            letterSpacing: "0.62em",
            color: C.AMBER,
            marginBottom: 22,
          }}
        >
          ENTROPY
        </div>
        <div
          style={{
            fontFamily: F.SERIF,
            fontSize: 54,
            lineHeight: 1.08,
            color: C.INK,
            maxWidth: 600,
          }}
        >
          A relational engine,
          <br />
          crash-tested by{" "}
          <span style={{ fontStyle: "italic", color: C.AMBER_BRIGHT }}>deterministic simulation</span>
        </div>
        <div
          style={{
            fontFamily: F.SANS,
            fontSize: 12.5,
            lineHeight: 1.55,
            color: C.INK_MUTED,
            maxWidth: 470,
            marginTop: 22,
          }}
        >
          Slotted-page storage, a latch-crabbing B+ tree, MVCC snapshot
          isolation, write-ahead logging, and ARIES-style recovery, written
          from scratch in C++20 and driven through seeded, replayable fault
          injection with invariant checks after every crash.
        </div>
      </div>

      {/* bottom strip */}
      <div
        style={{
          position: "absolute",
          left: 64,
          right: 64,
          bottom: 62,
          borderTop: `0.5px solid ${C.HAIRLINE}`,
          paddingTop: 12,
          display: "flex",
          gap: 34,
          fontFamily: F.MONO,
          fontSize: 8,
          letterSpacing: "0.1em",
          color: C.INK_SUBTLE,
        }}
      >
        <span>
          <span style={{ color: C.AMBER }}>9</span> crash schedules
        </span>
        <span>
          <span style={{ color: C.AMBER }}>360</span> crash-recover-verify cycles per push
        </span>
        <span>
          <span style={{ color: C.AMBER }}>2</span> bugs found, fixed, locked
        </span>
        <span style={{ marginLeft: "auto" }}>github.com/ShreeChaturvedi/entropy</span>
      </div>
    </Page>
  );
}
