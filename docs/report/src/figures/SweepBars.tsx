import { C } from "../theme";
import { FIG } from "./common";

/**
 * FIG 2.3 — seed sweeps before and after each fix. Bar widths are the
 * failing share of the recorded sweep.
 */
export function SweepBars() {
  const W = 650;
  const H = 196;
  const bx = 150;
  const bw = 400;
  const barH = 13;

  const Row = ({
    y,
    label,
    sub,
    failShare,
    right,
    rightColor,
  }: {
    y: number;
    label: string;
    sub: string;
    failShare: number;
    right: string;
    rightColor: string;
  }) => (
    <g>
      <text x={bx - 10} y={y + barH / 2 - 2} textAnchor="end" style={FIG.label}>
        {label}
      </text>
      <text x={bx - 10} y={y + barH / 2 + 9} textAnchor="end" style={FIG.labelSmall}>
        {sub}
      </text>
      <rect x={bx} y={y} width={bw} height={barH} fill={C.GREEN_TINT} stroke={C.GREEN} strokeWidth={0.5} />
      {failShare > 0 && (
        <rect x={bx} y={y} width={bw * failShare} height={barH} fill={C.RED_TINT} stroke={C.RED} strokeWidth={0.5} />
      )}
      <text x={bx + bw + 10} y={y + barH / 2 + 2.5} style={{ ...FIG.labelSmall, fill: rightColor }}>
        {right}
      </text>
    </g>
  );

  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      <text x={8} y={14} style={FIG.title}>
        FAILING SHARE OF EACH RECORDED SWEEP
      </text>

      <text x={8} y={42} style={{ ...FIG.labelStrong, fill: C.AMBER_BRIGHT }}>
        live_abort_repro
      </text>
      <Row y={52} label="before eebf21c" sub="issue #81" failShare={1} right="40/40 fail" rightColor={C.RED} />
      <Row y={80} label="after eebf21c" sub="PR #89" failShare={0} right="500/500 pass" rightColor={C.GREEN} />

      <text x={8} y={126} style={{ ...FIG.labelStrong, fill: C.AMBER_BRIGHT }}>
        torn_page_write
      </text>
      <Row y={136} label="before 8bd6db2" sub="issue #86" failShare={95 / 200} right="95/200 fail" rightColor={C.RED} />
      <Row y={164} label="after 8bd6db2" sub="PR #94" failShare={0} right="500/500 pass" rightColor={C.GREEN} />

      <line x1={bx} y1={182} x2={bx + bw} y2={182} stroke={C.HAIRLINE} strokeWidth={0.5} />
      <text x={bx} y={193} style={{ ...FIG.labelSmall, fill: C.INK_FAINT }}>
        0% of seeds
      </text>
      <text x={bx + bw} y={193} textAnchor="end" style={{ ...FIG.labelSmall, fill: C.INK_FAINT }}>
        100%
      </text>
    </svg>
  );
}
