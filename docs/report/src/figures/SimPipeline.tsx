import { C } from "../theme";
import { FIG, SvgBox, Arrow } from "./common";

/** FIG 1.2 — one run of run_schedule(seed, schedule). */
export function SimPipeline() {
  const W = 650;
  const H = 210;
  const y = 58;
  const h = 56;
  const stages = [
    { x: 8, w: 128, label: "live run", sub: "real stack on", sub2: "injecting devices", tone: "neutral" as const },
    { x: 168, w: 118, label: "crash()", sub: "resolve every", sub2: "unsynced write", tone: "red" as const },
    { x: 318, w: 118, label: "reopen", sub: "fault-free devices,", sub2: "recovery-mode pool", tone: "neutral" as const },
    { x: 468, w: 118, label: "recover()", sub: "analysis · redo", sub2: "· undo", tone: "cyan" as const },
  ];
  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      {/* top annotations */}
      <text x={8} y={16} style={FIG.title}>
        ONE SEED, ONE SCHEDULE · RUN_SCHEDULE()
      </text>
      <text x={8 + 64} y={y - 14} textAnchor="middle" style={FIG.labelSmall}>
        workload + oracle
      </text>
      <text x={168 + 59} y={y - 14} textAnchor="middle" style={{ ...FIG.labelSmall, fill: C.RED }}>
        lost · torn · kept
      </text>

      {stages.map((s) => (
        <SvgBox key={s.label} x={s.x} y={y} w={s.w} h={h} label={s.label} sub={s.sub} tone={s.tone} />
      ))}
      {stages.map((s) => (
        <text key={`${s.label}-2`} x={s.x + s.w / 2} y={y + h / 2 + 18} textAnchor="middle" style={FIG.labelSmall}>
          {s.sub2}
        </text>
      ))}
      <Arrow x1={136} y1={y + h / 2} x2={166} y2={y + h / 2} />
      <Arrow x1={286} y1={y + h / 2} x2={316} y2={y + h / 2} />
      <Arrow x1={436} y1={y + h / 2} x2={466} y2={y + h / 2} />

      {/* oracle verdict */}
      <SvgBox x={468} y={y + 92} w={118} h={40} label="oracle check" sub="invariants" tone="amber" />
      <Arrow x1={527} y1={y + h} x2={527} y2={y + 90} color={C.AMBER} />
      <text x={452} y={y + 116} textAnchor="end" style={{ ...FIG.labelSmall, fill: C.INK_MUTED }}>
        committed_present · no_uncommitted_visible
      </text>
      <text x={452} y={y + 127} textAnchor="end" style={FIG.labelSmall}>
        one JSONL line per run, byte-identical on replay
      </text>

      {/* damage detail under crash stage */}
      <text x={168} y={y + h + 26} style={FIG.labelSmall}>
        torn page: prefix or suffix survives,
      </text>
      <text x={168} y={y + h + 37} style={FIG.labelSmall}>
        boundary drawn from [1, 4095]
      </text>
      <text x={8} y={y + h + 26} style={FIG.labelSmall}>
        fsync&apos;d writes always survive;
      </text>
      <text x={8} y={y + h + 37} style={FIG.labelSmall}>
        only the unsynced are at risk
      </text>
    </svg>
  );
}
