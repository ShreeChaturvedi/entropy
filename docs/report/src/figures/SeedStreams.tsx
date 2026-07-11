import { C } from "../theme";
import { FIG, SvgBox, Arrow } from "./common";

/** FIG 1.1 — one 64-bit seed derives three independent PRNG streams. */
export function SeedStreams() {
  const W = 650;
  const H = 158;
  const rows = [
    { y: 16, salt: "0x1  workload", box: "std::mt19937_64", use: "RandomWorkload", sub: "op mix · abort draws" },
    { y: 62, salt: "0x2  disk", box: "std::mt19937_64", use: "SimDiskManager", sub: "page-write fates · IO errors" },
    { y: 108, salt: "0x3  log", box: "std::mt19937_64", use: "SimLogStore", sub: "WAL-tail fates" },
  ];
  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      <SvgBox x={8} y={60} w={118} h={38} label="seed" sub="uint64, from CLI" tone="amber" />
      {rows.map((r) => (
        <g key={r.salt}>
          <Arrow x1={126} y1={79} x2={196} y2={r.y + 17} />
          <SvgBox x={198} y={r.y} w={150} h={34} label={`splitmix64 ⊕ ${r.salt}`} tone="well" />
          <Arrow x1={348} y1={r.y + 17} x2={392} y2={r.y + 17} />
          <SvgBox x={394} y={r.y} w={104} h={34} label={r.box} />
          <Arrow x1={498} y1={r.y + 17} x2={528} y2={r.y + 17} />
          <text x={534} y={r.y + 14} style={FIG.labelStrong}>
            {r.use}
          </text>
          <text x={534} y={r.y + 25} style={FIG.labelSmall}>
            {r.sub}
          </text>
        </g>
      ))}
      <text x={198} y={H - 2} style={{ ...FIG.labelSmall, fill: C.INK_FAINT }}>
        derive_seed(seed, salt) · fault.hpp
      </text>
    </svg>
  );
}
