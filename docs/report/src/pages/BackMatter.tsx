import { Page } from "../components/Page";
import { Eyebrow, P, Code, Spacer, Cols, Note } from "../components/Blocks";
import { mulberry32 } from "../components/Divider";
import { C, F, G } from "../theme";

export function Reproduction() {
  return (
    <Page section="Appendix A · Reproduction" folio={23}>
      <Eyebrow>Appendix A</Eyebrow>
      <Spacer h={6} />
      <h1 style={{ fontFamily: F.SERIF, fontSize: 28, color: C.INK, margin: "0 0 18px" }}>
        Reproduction
      </h1>
      <Cols
        main={
          <>
            <P>
              Everything in this document regenerates from the repository.
              Dependencies (GoogleTest, Google Benchmark, spdlog) fetch on
              first configure; the SQLite comparison needs system SQLite3
              headers.
            </P>
            <Code title="build and test" fontSize={8}>
{`cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
ctest --test-dir build --output-on-failure -C Release`}
            </Code>
            <Code title="crash simulation · chapters 01-02" fontSize={8}>
{`entropy-sim --list                                # nine schedules
entropy-sim --seeds 200 --schedule mixed          # one JSONL line per run
entropy-sim --schedule live_abort_repro --seeds 40   # bug lock, ch 02 case I
entropy-sim --schedule torn_page_write  --seeds 40   # bug lock, ch 02 case II`}
            </Code>
            <Code title="benchmarks · chapter 06" fontSize={8}>
{`./scripts/bench/run.sh     # writes docs/benchmarks/runs/<timestamp>.json
# SQLite comparison is ON by default; needs sqlite3 headers`}
            </Code>
            <Code title="terminal UI · chapter 05" fontSize={8}>
{`cmake -S . -B build -DENTROPY_BUILD_TUI=ON
entropy-tui                                   # live interface
entropy-tui --capture-frame dashboard --size 122x66   # the stills used here`}
            </Code>
            <Code title="this document" fontSize={8}>
{`cd docs/report && npm install && npm run pdf
# -> docs/report/dist/entropy-report.pdf`}
            </Code>
          </>
        }
        rail={
          <>
            <Spacer h={4} />
            <Note label="record">
              Bugs: issues #75, #81, #86. Fixes: commits eebf21c (PR #89) and
              8bd6db2 (PR #94); checksums default-on in 62800ef (PR #96).
              Schedule sources: src/sim/schedule.cpp.
            </Note>
            <Note label="colophon">
              Set in Instrument Serif, Schibsted Grotesk, and IBM Plex Mono.
              Composed as fixed pages in React, printed to PDF through headless
              Chromium. Every diagram is hand-authored SVG; the terminal stills
              are deterministic ANSI captures. Source: docs/report/.
            </Note>
            <Note label="license">
              Engine and document: MIT. SQLite is public domain; comparisons
              use the system library.
            </Note>
          </>
        }
      />
    </Page>
  );
}

export function BackCover() {
  const rnd = mulberry32(0xE7);
  const dots = Array.from({ length: 420 }, (_, i) => {
    const x = rnd() * G.PAGE_W;
    const y = rnd() * G.PAGE_H;
    const isAmber = rnd() < 0.04;
    return (
      <circle
        key={i}
        cx={x}
        cy={y}
        r={rnd() < 0.86 ? 0.8 : 1.5}
        fill={isAmber ? C.AMBER : C.INK}
        opacity={0.04 + rnd() * 0.2}
      />
    );
  });
  return (
    <Page chrome={false}>
      <svg width={G.PAGE_W} height={G.PAGE_H} viewBox={`0 0 ${G.PAGE_W} ${G.PAGE_H}`} style={{ position: "absolute", inset: 0 }}>
        <rect width={G.PAGE_W} height={G.PAGE_H} fill={C.PAGE} />
        {dots}
      </svg>
      <div
        style={{
          position: "absolute",
          inset: 0,
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          gap: 18,
        }}
      >
        <div style={{ fontFamily: F.MONO, fontSize: 11, fontWeight: 600, letterSpacing: "0.62em", color: C.AMBER }}>
          ENTROPY
        </div>
        <div style={{ fontFamily: F.SERIF, fontStyle: "italic", fontSize: 15, color: C.INK_MUTED }}>
          Every committed row survives. Nothing uncommitted shows.
        </div>
        <div style={{ fontFamily: F.MONO, fontSize: 8, letterSpacing: "0.14em", color: C.INK_SUBTLE }}>
          github.com/ShreeChaturvedi/entropy
        </div>
      </div>
    </Page>
  );
}
