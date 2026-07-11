import { C } from "../theme";
import { FIG } from "./common";

/** FIG 3.3 — snapshot visibility over one RID's version chain. */
export function MvccTimeline() {
  const W = 650;
  const H = 208;
  const ax = 60;
  const aw = 470;
  const t0 = 10;
  const t1 = 50;
  const X = (t: number) => ax + ((t - t0) / (t1 - t0)) * aw;

  const bar = (y: number, from: number, to: number, label: string, tone: "green" | "cyan") => (
    <g>
      <rect
        x={X(from)}
        y={y}
        width={X(to) - X(from)}
        height={20}
        fill={tone === "green" ? C.GREEN_TINT : C.CYAN_TINT}
        stroke={tone === "green" ? C.GREEN : C.CYAN}
        strokeWidth={0.6}
      />
      <text x={X(from) + 6} y={y + 13} style={{ ...FIG.labelSmall, fill: C.INK_MUTED }}>
        {label}
      </text>
    </g>
  );

  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      <text x={8} y={14} style={FIG.title}>
        ONE RID · VERSION CHAIN AND TWO SNAPSHOTS
      </text>

      {/* axis */}
      <line x1={ax} y1={160} x2={ax + aw + 20} y2={160} stroke={C.HAIRLINE_STRONG} strokeWidth={0.6} />
      {[10, 20, 30, 40, 50].map((t) => (
        <g key={t}>
          <line x1={X(t)} y1={160} x2={X(t)} y2={164} stroke={C.INK_SUBTLE} strokeWidth={0.5} />
          <text x={X(t)} y={175} textAnchor="middle" style={{ ...FIG.labelSmall, fill: C.INK_FAINT }}>
            ts={t}
          </text>
        </g>
      ))}

      {/* versions */}
      <text x={8} y={62} style={FIG.labelSmall}>
        v1
      </text>
      {bar(48, 14, 33, "begin_ts=14 · end_ts=33", "green")}
      <text x={8} y={94} style={FIG.labelSmall}>
        v2
      </text>
      {bar(80, 33, 50, "begin_ts=33 · end_ts=∞", "cyan")}

      {/* snapshots */}
      <line x1={X(25)} y1={36} x2={X(25)} y2={160} stroke={C.AMBER} strokeWidth={0.8} strokeDasharray="4 3" />
      <text x={X(25)} y={30} textAnchor="middle" style={{ ...FIG.labelSmall, fill: C.AMBER_BRIGHT }}>
        T_a · start_ts=25 → sees v1
      </text>
      <line x1={X(41)} y1={116} x2={X(41)} y2={160} stroke={C.AMBER} strokeWidth={0.8} strokeDasharray="4 3" />
      <text x={X(41)} y={128} textAnchor="start" style={{ ...FIG.labelSmall, fill: C.AMBER_BRIGHT }}>
        T_b · start_ts=41 → sees v2
      </text>

      {/* rules */}
      <text x={8} y={194} style={{ ...FIG.labelSmall, fill: C.INK_SUBTLE }}>
        visible ⇔ begin_ts ≤ start_ts &lt; end_ts (own uncommitted writes always visible) · write-write: first updater wins,
      </text>
      <text x={8} y={205} style={{ ...FIG.labelSmall, fill: C.INK_SUBTLE }}>
        the second writer aborts on an uncommitted create or delete, a committed delete, or a committed create newer than its snapshot
      </text>
    </svg>
  );
}
