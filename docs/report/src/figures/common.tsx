import type { ReactNode } from "react";
import { C, F } from "../theme";

/* Shared primitives for hand-authored SVG figures. All figures draw on the
   panel background provided by <Figure>, so they only paint strokes and ink. */

export const FIG = {
  label: {
    fontFamily: F.MONO,
    fontSize: 7.5,
    fill: C.INK_MUTED,
  },
  labelSmall: {
    fontFamily: F.MONO,
    fontSize: 6.6,
    fill: C.INK_SUBTLE,
  },
  labelStrong: {
    fontFamily: F.MONO,
    fontSize: 7.5,
    fontWeight: 600,
    fill: C.INK,
  },
  title: {
    fontFamily: F.MONO,
    fontSize: 7,
    letterSpacing: "0.16em",
    fill: C.INK_SUBTLE,
  },
} as const;

export function SvgBox({
  x,
  y,
  w,
  h,
  label,
  sub,
  tone = "neutral",
  dashed = false,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  label: ReactNode;
  sub?: ReactNode;
  tone?: "neutral" | "amber" | "cyan" | "red" | "green" | "well";
  dashed?: boolean;
}) {
  const stroke =
    tone === "amber" ? C.AMBER : tone === "cyan" ? C.CYAN : tone === "red" ? C.RED : tone === "green" ? C.GREEN : C.HAIRLINE_STRONG;
  const fill =
    tone === "amber" ? C.AMBER_TINT : tone === "cyan" ? C.CYAN_TINT : tone === "red" ? C.RED_TINT : tone === "green" ? C.GREEN_TINT : tone === "well" ? C.WELL : "none";
  const ink = tone === "amber" ? C.AMBER_BRIGHT : tone === "cyan" ? C.CYAN : tone === "red" ? C.RED : tone === "green" ? C.GREEN : C.INK;
  return (
    <g>
      <rect
        x={x}
        y={y}
        width={w}
        height={h}
        fill={fill}
        stroke={stroke}
        strokeWidth={0.75}
        strokeDasharray={dashed ? "3 2.5" : undefined}
        rx={2}
      />
      <text
        x={x + w / 2}
        y={sub ? y + h / 2 - 4 : y + h / 2 + 2.5}
        textAnchor="middle"
        style={{ ...FIG.labelStrong, fill: ink }}
      >
        {label}
      </text>
      {sub && (
        <text x={x + w / 2} y={y + h / 2 + 8} textAnchor="middle" style={FIG.labelSmall}>
          {sub}
        </text>
      )}
    </g>
  );
}

export function Arrow({
  x1,
  y1,
  x2,
  y2,
  color = C.INK_SUBTLE,
  dashed = false,
}: {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
  color?: string;
  dashed?: boolean;
}) {
  const angle = Math.atan2(y2 - y1, x2 - x1);
  const a1 = angle + Math.PI * 0.82;
  const a2 = angle - Math.PI * 0.82;
  const s = 4.5;
  return (
    <g>
      <line
        x1={x1}
        y1={y1}
        x2={x2}
        y2={y2}
        stroke={color}
        strokeWidth={0.75}
        strokeDasharray={dashed ? "3 2.5" : undefined}
      />
      <path
        d={`M ${x2} ${y2} L ${x2 + s * Math.cos(a1)} ${y2 + s * Math.sin(a1)} M ${x2} ${y2} L ${x2 + s * Math.cos(a2)} ${y2 + s * Math.sin(a2)}`}
        stroke={color}
        strokeWidth={0.75}
        fill="none"
      />
    </g>
  );
}
