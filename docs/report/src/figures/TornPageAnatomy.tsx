import { C } from "../theme";
import { FIG } from "./common";

/**
 * FIG 2.2 — anatomy of the fatal tear. Region widths are schematic, not to
 * byte scale; byte offsets are ticked underneath.
 */
export function TornPageAnatomy() {
  const W = 650;
  const H = 262;
  const bx = 8;
  const rowH = 40;

  // Schematic display segments: [x-offset, width, byte-start]
  const SEG = {
    hdr: { x: 0, w: 64 },
    slot: { x: 64, w: 104 },
    free: { x: 168, w: 148 },
    row: { x: 316, w: 84 },
  };
  const BW = 400;
  const tearX = bx + SEG.slot.x + SEG.slot.w; // tear at byte 128

  const Ticks = ({ y }: { y: number }) => (
    <g>
      {[
        { x: 0, t: "0" },
        { x: SEG.slot.x, t: "32" },
        { x: SEG.free.x, t: "128" },
        { x: SEG.row.x, t: "3970" },
        { x: BW, t: "4096" },
      ].map((tick) => (
        <g key={tick.t}>
          <line x1={bx + tick.x} y1={y} x2={bx + tick.x} y2={y + 3} stroke={C.INK_SUBTLE} strokeWidth={0.5} />
          <text x={bx + tick.x} y={y + 12} textAnchor="middle" style={{ ...FIG.labelSmall, fill: C.INK_FAINT }}>
            {tick.t}
          </text>
        </g>
      ))}
    </g>
  );

  const Region = ({
    y,
    seg,
    fill,
    stroke,
    label,
    hatch,
  }: {
    y: number;
    seg: { x: number; w: number };
    fill: string;
    stroke?: string;
    label?: string;
    hatch?: boolean;
  }) => (
    <g>
      <rect x={bx + seg.x} y={y} width={seg.w} height={rowH} fill={fill} stroke={stroke ?? "none"} strokeWidth={0.5} />
      {hatch &&
        Array.from({ length: Math.floor(seg.w / 8) }, (_, k) => (
          <line
            key={k}
            x1={bx + seg.x + k * 8}
            y1={y + rowH}
            x2={bx + seg.x + k * 8 + 7}
            y2={y}
            stroke={C.RED}
            strokeWidth={0.4}
            opacity={0.45}
          />
        ))}
      {label && (
        <text
          x={bx + seg.x + seg.w / 2}
          y={y + rowH / 2 + 2.5}
          textAnchor="middle"
          style={{ ...FIG.labelSmall, fill: C.INK_MUTED }}
        >
          {label}
        </text>
      )}
    </g>
  );

  const y1 = 44;
  const y2 = 128;

  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      <text x={bx} y={14} style={FIG.title}>
        ONE 4096-BYTE PAGE WRITE, TORN AT BYTE 128 · WIDTHS SCHEMATIC
      </text>

      <text x={bx} y={y1 - 7} style={FIG.labelSmall}>
        intended image (committed insert applied)
      </text>
      <rect x={bx} y={y1} width={BW} height={rowH} fill="none" stroke={C.HAIRLINE_STRONG} strokeWidth={0.75} />
      <Region y={y1} seg={SEG.hdr} fill={C.AMBER_TINT} stroke={C.AMBER} label="header" />
      <Region y={y1} seg={SEG.slot} fill={C.CYAN_TINT} stroke={C.CYAN} label="slot directory" />
      <Region y={y1} seg={SEG.free} fill="none" label="free space" />
      <Region y={y1} seg={SEG.row} fill={C.GREEN_TINT} stroke={C.GREEN} label="row bytes" />
      <Ticks y={y1 + rowH} />

      <text x={bx} y={y2 - 7} style={FIG.labelSmall}>
        what the disk kept
      </text>
      <rect x={bx} y={y2} width={BW} height={rowH} fill="none" stroke={C.HAIRLINE_STRONG} strokeWidth={0.75} />
      <Region y={y2} seg={SEG.hdr} fill={C.AMBER_TINT} stroke={C.AMBER} label="header" />
      <Region y={y2} seg={SEG.slot} fill={C.CYAN_TINT} stroke={C.CYAN} label="slot directory" />
      <Region y={y2} seg={{ x: SEG.free.x, w: BW - SEG.free.x }} fill={C.RED_TINT} label="zeros · committed row gone" hatch />
      <Ticks y={y2 + rowH} />

      {/* tear marker spanning both bars */}
      <line x1={tearX} y1={y1 - 16} x2={tearX} y2={y2 + rowH + 16} stroke={C.RED} strokeWidth={0.9} strokeDasharray="4 3" />
      <text x={tearX + 5} y={y1 - 18} style={{ ...FIG.labelSmall, fill: C.RED }}>
        tear boundary
      </text>

      {/* consequence panel */}
      <g>
        <text x={444} y={y1 + 4} style={{ ...FIG.labelStrong, fill: C.INK }}>
          why redo cannot repair it
        </text>
        <text x={444} y={y1 + 20} style={FIG.labelSmall}>
          · header still says TABLE_PAGE,
        </text>
        <text x={452} y={y1 + 31} style={FIG.labelSmall}>
          so redo never re-initializes
        </text>
        <text x={444} y={y1 + 46} style={FIG.labelSmall}>
          · slot claims the record is live, so
        </text>
        <text x={452} y={y1 + 57} style={FIG.labelSmall}>
          insert_record_at refuses the slot
        </text>
        <text x={444} y={y1 + 72} style={FIG.labelSmall}>
          · nothing else validates the page
        </text>
        <text x={444} y={y2 + 14} style={{ ...FIG.labelStrong, fill: C.RED }}>
          recover() returns ok
        </text>
        <text x={444} y={y2 + 26} style={FIG.labelSmall}>
          redo=0 · undo=0
        </text>
        <text x={444} y={y2 + 37} style={FIG.labelSmall}>
          the slot serves zeros
        </text>
      </g>

      {/* fix strip */}
      <line x1={8} y1={202} x2={W - 8} y2={202} stroke={C.HAIRLINE} strokeWidth={0.5} />
      <text x={bx} y={222} style={{ ...FIG.title, fill: C.CYAN }}>
        AFTER 8bd6db2 · CRC-32 AT HEADER BYTES [12,16)
      </text>
      <text x={bx} y={238} style={FIG.labelSmall}>
        read_page recomputes the checksum → mismatch → Status::Corruption("torn/corrupt page: checksum mismatch")
      </text>
      <text x={bx} y={251} style={FIG.labelSmall}>
        the recovery-mode pool reinitializes the frame (LSN 0) → repeat-history redo rebuilds every committed row from the WAL
      </text>
    </svg>
  );
}
