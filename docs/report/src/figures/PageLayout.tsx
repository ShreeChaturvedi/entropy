import { C } from "../theme";
import { FIG, Arrow } from "./common";

/** FIG 3.1 — the 4096-byte slotted page and its 32-byte header. */
export function PageLayout() {
  const W = 650;
  const H = 252;
  const px = 8;
  const py = 26;
  const pw = 236;

  const sections = [
    { h: 34, label: "page header · bytes [0, 32)", fill: C.AMBER_TINT, stroke: C.AMBER },
    { h: 44, label: "slot array · 4 B per slot", fill: C.CYAN_TINT, stroke: C.CYAN, arrow: "down" },
    { h: 84, label: "free space", fill: "none", stroke: undefined },
    { h: 48, label: "records", fill: C.GREEN_TINT, stroke: C.GREEN, arrow: "up" },
  ];

  const fields = [
    { o: "0", f: "lsn", s: "8", note: "redo gate" },
    { o: "8", f: "page_id", s: "4", note: "" },
    { o: "12", f: "checksum", s: "4", note: "CRC-32, IEEE 0xEDB88320", hot: true },
    { o: "16", f: "record_count", s: "2", note: "" },
    { o: "18", f: "free_space_offset", s: "2", note: "top of slot array" },
    { o: "20", f: "free_space_end", s: "2", note: "bottom of records" },
    { o: "22", f: "page_type", s: "1", note: "" },
    { o: "23", f: "reserved", s: "9", note: "heap next/prev links" },
  ];

  let y = py;
  const rendered = sections.map((s) => {
    const el = (
      <g key={s.label}>
        <rect x={px} y={y} width={pw} height={s.h} fill={s.fill} stroke={s.stroke ?? C.HAIRLINE_STRONG} strokeWidth={0.6} />
        <text x={px + 10} y={y + 15} style={{ ...FIG.label, fill: C.INK_MUTED }}>
          {s.label}
        </text>
        {s.arrow === "down" && <Arrow x1={px + pw - 16} y1={y + 10} x2={px + pw - 16} y2={y + s.h - 8} color={C.CYAN} />}
        {s.arrow === "up" && <Arrow x1={px + pw - 16} y1={y + s.h - 8} x2={px + pw - 16} y2={y + 10} color={C.GREEN} />}
      </g>
    );
    y += s.h;
    return el;
  });

  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      <text x={px} y={14} style={FIG.title}>
        4096-BYTE PAGE · SLOTS GROW DOWN, RECORDS GROW UP
      </text>
      {rendered}
      <rect x={px} y={py} width={pw} height={y - py} fill="none" stroke={C.HAIRLINE_STRONG} strokeWidth={0.8} />
      <text x={px} y={y + 13} style={{ ...FIG.labelSmall, fill: C.INK_FAINT }}>
        free space = free_space_end − free_space_offset (contiguous)
      </text>

      {/* header field table */}
      <text x={300} y={py + 4} style={FIG.title}>
        PAGEHEADER · 32 BYTES, STATIC_ASSERT&apos;D
      </text>
      <line x1={300} y1={py + 11} x2={W - 8} y2={py + 11} stroke={C.HAIRLINE_STRONG} strokeWidth={0.5} />
      {fields.map((f, i) => {
        const fy = py + 26 + i * 20;
        return (
          <g key={f.f}>
            <text x={300} y={fy} style={{ ...FIG.labelSmall, fill: C.INK_FAINT }}>
              {f.o}
            </text>
            <text x={326} y={fy} style={{ ...FIG.labelStrong, fill: f.hot ? C.AMBER_BRIGHT : C.INK }}>
              {f.f}
            </text>
            <text x={438} y={fy} style={FIG.labelSmall}>
              {f.s} B
            </text>
            <text x={470} y={fy} style={{ ...FIG.labelSmall, fill: f.hot ? C.AMBER : C.INK_SUBTLE }}>
              {f.note}
            </text>
            <line x1={300} y1={fy + 6} x2={W - 8} y2={fy + 6} stroke={C.HAIRLINE} strokeWidth={0.4} />
          </g>
        );
      })}
      <text x={300} y={py + 26 + fields.length * 20 + 8} style={{ ...FIG.labelSmall, fill: C.INK_SUBTLE }}>
        checksum covers the full image with bytes [12,16) carved out;
      </text>
      <text x={300} y={py + 26 + fields.length * 20 + 19} style={{ ...FIG.labelSmall, fill: C.INK_SUBTLE }}>
        an all-zero page verifies clean by rule (fresh or deallocated)
      </text>
    </svg>
  );
}
