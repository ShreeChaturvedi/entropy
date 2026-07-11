import { C } from "../theme";
import { FIG, Arrow } from "./common";

/** FIG 3.4 — the three recovery passes over one damaged log. */
export function RecoveryPhases() {
  const W = 650;
  const H = 226;
  const y = 66;
  const h = 26;

  const recs = [
    { x: 8, w: 78, label: "CHECKPOINT", tone: C.AMBER, fill: C.AMBER_TINT },
    { x: 90, w: 62, label: "BEGIN t7", tone: C.HAIRLINE_STRONG, fill: "none" },
    { x: 156, w: 68, label: "INSERT t7", tone: C.GREEN, fill: C.GREEN_TINT },
    { x: 228, w: 66, label: "COMMIT t7", tone: C.GREEN, fill: C.GREEN_TINT },
    { x: 298, w: 62, label: "BEGIN t9", tone: C.HAIRLINE_STRONG, fill: "none" },
    { x: 364, w: 68, label: "INSERT t9", tone: C.RED, fill: C.RED_TINT },
    { x: 436, w: 68, label: "UPDATE t9", tone: C.RED, fill: C.RED_TINT },
  ];

  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      <text x={8} y={14} style={FIG.title}>
        THE DURABLE LOG AFTER A CRASH · t7 COMMITTED, t9 IN FLIGHT
      </text>

      {recs.map((r) => (
        <g key={r.label}>
          <rect x={r.x} y={y} width={r.w} height={h} fill={r.fill} stroke={r.tone} strokeWidth={0.7} rx={2} />
          <text x={r.x + r.w / 2} y={y + 16.5} textAnchor="middle" style={{ ...FIG.labelSmall, fill: C.INK_MUTED }}>
            {r.label}
          </text>
        </g>
      ))}
      {/* crash cut */}
      <line x1={512} y1={y - 12} x2={512} y2={y + h + 12} stroke={C.RED} strokeWidth={1} strokeDasharray="4 3" />
      <text x={518} y={y + 6} style={{ ...FIG.labelSmall, fill: C.RED }}>
        crash: unsynced
      </text>
      <text x={518} y={y + 17} style={{ ...FIG.labelSmall, fill: C.RED }}>
        tail lost or torn
      </text>

      {/* phase 1: analysis */}
      <Arrow x1={12} y1={y + h + 24} x2={506} y2={y + h + 24} color={C.INK_SUBTLE} />
      <text x={12} y={y + h + 38} style={{ ...FIG.labelStrong, fill: C.INK }}>
        1 · analysis
      </text>
      <text x={86} y={y + h + 38} style={FIG.labelSmall}>
        forward from the last CHECKPOINT: rebuild the active-transaction table, set the redo start LSN.
      </text>
      <text x={86} y={y + h + 49} style={FIG.labelSmall}>
        t7 leaves the table at COMMIT; t9 never does — t9 is the loser.
      </text>

      {/* phase 2: redo */}
      <Arrow x1={12} y1={y + h + 66} x2={506} y2={y + h + 66} color={C.CYAN} />
      <text x={12} y={y + h + 80} style={{ ...FIG.labelStrong, fill: C.CYAN }}>
        2 · redo
      </text>
      <text x={86} y={y + h + 80} style={FIG.labelSmall}>
        repeat history for every data record, committed or not, skipping any page whose LSN ≥ the record&apos;s.
      </text>
      <text x={86} y={y + h + 91} style={FIG.labelSmall}>
        the gate makes redo idempotent: recovery is safe to re-run on its own partial results.
      </text>

      {/* phase 3: undo */}
      <Arrow x1={506} y1={y + h + 108} x2={12} y2={y + h + 108} color={C.AMBER} />
      <text x={12} y={y + h + 122} style={{ ...FIG.labelStrong, fill: C.AMBER_BRIGHT }}>
        3 · undo
      </text>
      <text x={86} y={y + h + 122} style={FIG.labelSmall}>
        reverse-LSN scan over the losers&apos; records: state-checked inverses remove t9&apos;s effects,
      </text>
      <text x={86} y={y + h + 133} style={FIG.labelSmall}>
        compensated pages flush before the ABORT records append.
      </text>
    </svg>
  );
}
