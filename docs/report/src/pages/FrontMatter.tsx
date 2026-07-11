import { Page } from "../components/Page";
import { Eyebrow, Lede, P, Spacer } from "../components/Blocks";
import { C, F } from "../theme";

const stats = [
  { n: "22,500", label: "non-empty lines of C++20, all first-party" },
  { n: "9 × 40", label: "schedules × seeds swept on every CI push" },
  { n: "95 / 200", label: "seeds failing before the torn-page fix; 0 after" },
  { n: "0.90x", label: "vs SQLite, 1k-row transactional insert batch (M2)" },
];

const toc = [
  { n: "01", t: "Crash recovery as a falsifiable claim", p: "04" },
  { n: "02", t: "Two bugs the test suite never caught", p: "07" },
  { n: "03", t: "The engine under test", p: "12" },
  { n: "04", t: "From SQL text to a costed plan", p: "16" },
  { n: "05", t: "An instrument panel for crashes", p: "18" },
  { n: "06", t: "Measured, on two machines", p: "20" },
  { n: "07", t: "What this engine does not do", p: "22" },
  { n: "A", t: "Reproduction and colophon", p: "23" },
];

export function Abstract() {
  return (
    <Page section="Abstract" folio={2}>
      <Eyebrow>Abstract</Eyebrow>
      <Spacer h={14} />
      <div style={{ maxWidth: 560 }}>
        <Lede>
          Entropy is a relational database engine written from scratch in
          C++20: slotted-page storage under a latch-crabbing B+ tree, MVCC
          snapshot isolation, write-ahead logging, and ARIES-style three-pass
          recovery, fronted by a SQL parser, a cost-based planner, and volcano
          executors.
        </Lede>
        <P style={{ fontSize: 11, lineHeight: 1.62 }}>
          This document examines the claim databases find hardest to earn:
          that a crash at any instant loses nothing committed and leaks
          nothing uncommitted. Entropy tests it the way FoundationDB does, with
          a deterministic simulator that runs the production stack over
          fault-injecting storage devices. One 64-bit seed derives independent
          PRNG streams for the workload, the page device, and the log store; a
          crash resolves every unsynced write to lost, torn, or kept; recovery
          runs on the damaged image; and an oracle checks every committed row,
          byte for byte, against what must have survived. A failing seed
          replays identically until it is understood.
        </P>
        <P style={{ fontSize: 11, lineHeight: 1.62 }}>
          The method paid for itself immediately. Seed sweeps surfaced two
          crash-path bugs that more than 550 passing unit and integration tests
          had never touched: cleanly aborted transactions resurrected by
          repeat-history redo, and torn page writes that recovery signed off as
          success while committed bytes read back as zeros. Both are analyzed
          here to root cause, both are fixed (compensation log records on the
          abort path; <span style={{ whiteSpace: "nowrap" }}>CRC-32</span> page
          checksums with a recovery-mode rebuild), and
          both fixes are locked by the schedules that found them, re-verified
          across 40 seeds on every push. The engine holds 0.90x SQLite on
          1k-row transactional insert batches on the committed benchmark run,
          and chapter 07 states plainly what does not work yet.
        </P>
      </div>

      {/* stat row */}
      <Spacer h={16} />
      <div style={{ display: "flex", gap: 14 }}>
        {stats.map((s) => (
          <div
            key={s.label}
            style={{
              flex: 1,
              border: `0.5px solid ${C.HAIRLINE}`,
              background: C.PANEL,
              borderRadius: 3,
              padding: "12px 14px",
            }}
          >
            <div style={{ fontFamily: F.MONO, fontSize: 17, fontWeight: 600, color: C.AMBER_BRIGHT }}>{s.n}</div>
            <div style={{ fontFamily: F.MONO, fontSize: 6.8, lineHeight: 1.5, color: C.INK_SUBTLE, marginTop: 5 }}>
              {s.label}
            </div>
          </div>
        ))}
      </div>

      {/* contents */}
      <Spacer h={30} />
      <Eyebrow color={C.INK_SUBTLE}>Contents</Eyebrow>
      <Spacer h={8} />
      <div style={{ maxWidth: 560 }}>
        {toc.map((row) => (
          <div
            key={row.n}
            style={{
              display: "flex",
              alignItems: "baseline",
              gap: 16,
              padding: "7.5px 0",
              borderBottom: `0.5px solid ${C.HAIRLINE}`,
            }}
          >
            <span style={{ fontFamily: F.MONO, fontSize: 8.5, color: C.AMBER, width: 18 }}>{row.n}</span>
            <span style={{ fontFamily: F.SANS, fontSize: 11.5, fontWeight: 500, color: C.INK }}>{row.t}</span>
            <span style={{ flex: 1, borderBottom: `0.5px dotted ${C.HAIRLINE}`, transform: "translateY(-3px)" }} />
            <span style={{ fontFamily: F.MONO, fontSize: 8.5, color: C.INK_SUBTLE }}>{row.p}</span>
          </div>
        ))}
      </div>
      <P style={{ marginTop: 14, fontSize: 8.5, fontFamily: F.MONO, color: C.INK_FAINT, textAlign: "left" }}>
        All figures are drawn from the repository: source, issues, pull
        requests, commit history, committed benchmark runs, and live output of
        the referenced builds.
      </P>
    </Page>
  );
}
