import { C } from "../theme";
import { FIG } from "./common";

/**
 * FIG 6.1 — Entropy vs SQLite, per-op time from the committed M2 run file
 * (docs/benchmarks/runs/bench-20251226-214051.json). Two panels because the
 * two workloads live at different magnitudes.
 */
export function BenchChart() {
  const W = 650;
  const H = 232;

  const Panel = ({
    x0,
    title,
    unit,
    max,
    rows,
  }: {
    x0: number;
    title: string;
    unit: string;
    max: number;
    rows: { label: string; e: number; s: number; ratio: string }[];
  }) => {
    const bw = 180;
    const bx = x0 + 64;
    const barH = 11;
    return (
      <g>
        <text x={x0} y={40} style={{ ...FIG.labelStrong, fill: C.INK }}>
          {title}
        </text>
        <text x={x0} y={52} style={FIG.labelSmall}>
          {unit} · lower is better
        </text>
        {rows.map((r, i) => {
          const y = 72 + i * 58;
          return (
            <g key={r.label}>
              <text x={x0} y={y + 9} style={FIG.labelSmall}>
                {r.label}
              </text>
              <rect x={bx} y={y} width={(r.e / max) * bw} height={barH} fill={C.AMBER_TINT} stroke={C.AMBER} strokeWidth={0.6} />
              <text x={bx + (r.e / max) * bw + 5} y={y + 9} style={{ ...FIG.labelSmall, fill: C.AMBER_BRIGHT }}>
                {r.e}
              </text>
              <rect x={bx} y={y + 16} width={(r.s / max) * bw} height={barH} fill={C.CYAN_TINT} stroke={C.CYAN} strokeWidth={0.6} />
              <text x={bx + (r.s / max) * bw + 5} y={y + 25} style={{ ...FIG.labelSmall, fill: C.CYAN }}>
                {r.s}
              </text>
              <text x={bx} y={y + 42} style={{ ...FIG.labelSmall, fill: C.INK_SUBTLE }}>
                ratio {r.ratio}
              </text>
            </g>
          );
        })}
      </g>
    );
  };

  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      <text x={8} y={14} style={FIG.title}>
        APPLE M2 · RELEASE -O3 · RUN FILE bench-20251226-214051.json
      </text>
      {/* legend */}
      <rect x={468} y={6} width={9} height={9} fill={C.AMBER_TINT} stroke={C.AMBER} strokeWidth={0.6} />
      <text x={482} y={14} style={FIG.labelSmall}>
        entropy
      </text>
      <rect x={534} y={6} width={9} height={9} fill={C.CYAN_TINT} stroke={C.CYAN} strokeWidth={0.6} />
      <text x={548} y={14} style={FIG.labelSmall}>
        sqlite 3
      </text>

      <Panel
        x0={8}
        title="insert batch, one txn"
        unit="ms per batch"
        max={10}
        rows={[
          { label: "1k rows", e: 0.94, s: 1.05, ratio: "0.90x — entropy ahead" },
          { label: "10k rows", e: 9.69, s: 6.99, ratio: "1.39x" },
        ]}
      />
      <Panel
        x0={340}
        title="point select, no index"
        unit="µs per query (full scan)"
        max={470}
        rows={[
          { label: "1k rows", e: 46.0, s: 23.0, ratio: "2.00x" },
          { label: "10k rows", e: 459.7, s: 179.8, ratio: "2.56x" },
        ]}
      />
    </svg>
  );
}
