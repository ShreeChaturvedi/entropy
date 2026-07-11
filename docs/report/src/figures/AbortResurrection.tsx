import { C } from "../theme";
import { FIG, SvgBox, Arrow } from "./common";

/**
 * FIG 2.1 — how repeat-history redo resurrected a cleanly aborted insert,
 * and how a compensation record removes it. Two lanes: before / after fix.
 */
export function AbortResurrection() {
  const W = 650;
  const H = 268;

  const walY = (base: number) => base;
  const recs = (base: number, withClr: boolean) => {
    const items = withClr
      ? [
          { x: 8, w: 74, label: "BEGIN t", tone: "neutral" as const },
          { x: 86, w: 96, label: "INSERT r", tone: "amber" as const },
          { x: 186, w: 110, label: "CLR: DELETE r", tone: "cyan" as const },
          { x: 300, w: 74, label: "ABORT t", tone: "neutral" as const },
        ]
      : [
          { x: 8, w: 74, label: "BEGIN t", tone: "neutral" as const },
          { x: 86, w: 96, label: "INSERT r", tone: "amber" as const },
          { x: 186, w: 74, label: "ABORT t", tone: "neutral" as const },
        ];
    return items.map((rec) => (
      <SvgBox key={rec.label} x={rec.x} y={walY(base)} w={rec.w} h={26} label={rec.label} tone={rec.tone} />
    ));
  };

  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      {/* ── lane 1: before ── */}
      <text x={8} y={14} style={{ ...FIG.title, fill: C.RED }}>
        BEFORE · ABORT LOGS NO COMPENSATION
      </text>
      <text x={8} y={30} style={FIG.labelSmall}>
        durable WAL
      </text>
      {recs(38, false)}
      <text x={272} y={55} style={FIG.labelSmall}>
        abort undid the page in memory,
      </text>
      <text x={272} y={66} style={FIG.labelSmall}>
        but the log holds no trace of the undo
      </text>

      <Arrow x1={480} y1={51} x2={528} y2={51} color={C.RED} />
      <text x={504} y={30} textAnchor="middle" style={{ ...FIG.labelSmall, fill: C.INK_SUBTLE }}>
        crash loses the
      </text>
      <text x={504} y={41} textAnchor="middle" style={{ ...FIG.labelSmall, fill: C.INK_SUBTLE }}>
        compensated page
      </text>
      <SvgBox x={534} y={38} w={108} h={26} label="redo: INSERT r" tone="red" />
      <text x={534} y={80} style={{ ...FIG.labelStrong, fill: C.RED }}>
        row r is back.
      </text>
      <text x={534} y={92} style={FIG.labelSmall}>
        t is not a loser (ABORT is
      </text>
      <text x={534} y={102} style={FIG.labelSmall}>
        durable), so undo never runs
      </text>

      {/* divider */}
      <line x1={8} y1={126} x2={W - 8} y2={126} stroke={C.HAIRLINE} strokeWidth={0.5} />

      {/* ── lane 2: after ── */}
      <text x={8} y={148} style={{ ...FIG.title, fill: C.CYAN }}>
        AFTER · eebf21c: ONE REDOABLE CLR PER UNDONE WRITE
      </text>
      <text x={8} y={164} style={FIG.labelSmall}>
        durable WAL
      </text>
      {recs(172, true)}
      <text x={8} y={218} style={FIG.labelSmall}>
        undone INSERT → CLR DELETE at the same rid · undone DELETE → CLR INSERT of the before-image
      </text>
      <text x={8} y={229} style={FIG.labelSmall}>
        undone UPDATE → CLR UPDATE back to the before-image · compensation durable before ABORT appends
      </text>

      <Arrow x1={480} y1={185} x2={528} y2={185} color={C.CYAN} />
      <text x={504} y={164} textAnchor="middle" style={{ ...FIG.labelSmall, fill: C.INK_SUBTLE }}>
        same crash,
      </text>
      <text x={504} y={175} textAnchor="middle" style={{ ...FIG.labelSmall, fill: C.INK_SUBTLE }}>
        same lost page
      </text>
      <SvgBox x={534} y={156} w={108} h={26} label="redo: INSERT r" tone="neutral" />
      <SvgBox x={534} y={188} w={108} h={26} label="redo: DELETE r" tone="cyan" />
      <text x={534} y={232} style={{ ...FIG.labelStrong, fill: C.GREEN }}>
        history repeats,
      </text>
      <text x={534} y={244} style={{ ...FIG.labelStrong, fill: C.GREEN }}>
        inverse included.
      </text>
      <text x={534} y={256} style={FIG.labelSmall}>
        no trace of t survives
      </text>
    </svg>
  );
}
