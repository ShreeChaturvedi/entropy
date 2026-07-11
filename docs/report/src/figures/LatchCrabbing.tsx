import { C } from "../theme";
import { FIG } from "./common";

/** FIG 3.2 — latch crabbing: a writer descending while readers scan. */
export function LatchCrabbing() {
  const W = 650;
  const H = 236;

  const node = (x: number, y: number, w: number, label: string, latch?: "W" | "R" | "released") => (
    <g key={`${x}-${y}`}>
      <rect
        x={x}
        y={y}
        width={w}
        height={26}
        fill={latch === "W" ? C.AMBER_TINT : latch === "R" ? C.CYAN_TINT : "none"}
        stroke={latch === "W" ? C.AMBER : latch === "R" ? C.CYAN : C.HAIRLINE_STRONG}
        strokeWidth={latch === "released" ? 0.5 : 0.8}
        strokeDasharray={latch === "released" ? "3 2.5" : undefined}
        rx={2}
      />
      <text x={x + w / 2} y={y + 16.5} textAnchor="middle" style={{ ...FIG.labelStrong, fill: latch === "W" ? C.AMBER_BRIGHT : latch === "R" ? C.CYAN : C.INK_MUTED }}>
        {label}
      </text>
      {latch && latch !== "released" && (
        <text x={x + w + 5} y={y + 10} style={{ ...FIG.labelSmall, fill: latch === "W" ? C.AMBER : C.CYAN }}>
          {latch}
        </text>
      )}
    </g>
  );

  const edge = (x1: number, y1: number, x2: number, y2: number) => (
    <line key={`${x1}-${x2}-${y2}`} x1={x1} y1={y1} x2={x2} y2={y2} stroke={C.HAIRLINE_STRONG} strokeWidth={0.5} />
  );

  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      <text x={8} y={14} style={FIG.title}>
        WRITER (SERIALIZED, WRITE LATCHES) AND READERS (CONCURRENT, READ LATCHES)
      </text>

      {/* tree */}
      {node(150, 34, 80, "root", "released")}
      {edge(190, 60, 120, 88)}
      {edge(190, 60, 262, 88)}
      {node(80, 88, 80, "internal A", "released")}
      {node(222, 88, 80, "internal B", "W")}
      {edge(120, 114, 70, 148)}
      {edge(120, 114, 150, 148)}
      {edge(262, 114, 240, 148)}
      {edge(262, 114, 330, 148)}
      {node(34, 148, 72, "leaf 1", "R")}
      {node(118, 148, 72, "leaf 2", "R")}
      {node(206, 148, 72, "leaf 3", "W")}
      {node(292, 148, 72, "leaf 4", "W")}

      {/* leaf chain */}
      {[106, 190, 278].map((x) => (
        <g key={x}>
          <line x1={x} y1={161} x2={x + 12} y2={161} stroke={C.INK_SUBTLE} strokeWidth={0.6} />
          <path d={`M ${x + 12} 161 l -3.5 -2.5 M ${x + 12} 161 l -3.5 2.5`} stroke={C.INK_SUBTLE} strokeWidth={0.6} fill="none" />
        </g>
      ))}
      <text x={34} y={192} style={FIG.labelSmall}>
        chain scan: holds two leaves, waits rightward only
      </text>

      {/* annotations */}
      <text x={412} y={44} style={{ ...FIG.labelStrong, fill: C.AMBER_BRIGHT }}>
        writer, splitting leaf 3
      </text>
      <text x={412} y={58} style={FIG.labelSmall}>
        · one structural writer at a time
      </text>
      <text x={412} y={70} style={FIG.labelSmall}>
        · crabs down, sheds every ancestor
      </text>
      <text x={420} y={81} style={FIG.labelSmall}>
        above the deepest unsafe node
      </text>
      <text x={412} y={93} style={FIG.labelSmall}>
        · keeps parent B latched: readers are
      </text>
      <text x={420} y={104} style={FIG.labelSmall}>
        fenced out of the whole subtree
      </text>
      <text x={412} y={122} style={{ ...FIG.labelStrong, fill: C.CYAN }}>
        readers
      </text>
      <text x={412} y={136} style={FIG.labelSmall}>
        · parent + child during descent
      </text>
      <text x={412} y={148} style={FIG.labelSmall}>
        · two adjacent leaves during scans
      </text>
      <text x={412} y={160} style={FIG.labelSmall}>
        · never latch internal pages sideways
      </text>

      <line x1={8} y1={206} x2={W - 8} y2={206} stroke={C.HAIRLINE} strokeWidth={0.5} />
      <text x={8} y={222} style={{ ...FIG.labelSmall, fill: C.INK_SUBTLE }}>
        deadlock freedom: all leaf-latch waits run left to right (a left turn first releases its right-sibling latch);
      </text>
      <text x={8} y={233} style={{ ...FIG.labelSmall, fill: C.INK_SUBTLE }}>
        a grown root is published unlatched via a release-store of the root id, readers observe it with an acquire load
      </text>
    </svg>
  );
}
