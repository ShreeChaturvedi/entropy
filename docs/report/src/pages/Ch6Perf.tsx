import { Page } from "../components/Page";
import {
  ChapterOpen,
  Cols,
  P,
  Lede,
  H2,
  M,
  Note,
  Figure,
  Tbl,
  Spacer,
  Strong,
} from "../components/Blocks";
import { BenchChart } from "../figures/BenchChart";

const SECTION = "06 · Performance";

export const ch6Pages = [
  <Page section={SECTION} key="ch6-1">
    <ChapterOpen index="06" title="Measured, on two machines" kicker="Benchmarks" />
    <Cols
      main={
        <>
          <Lede>
            Two kinds of speed matter to this project, and they are not the
            same. The first is how fast the simulator can interrogate the
            engine: on a mobile i7-1165G7, a 200-seed <M>mixed</M> sweep
            completes in 0.398 seconds of wall clock, about 500 full
            crash-recover-verify cycles per second, each cycle pushing roughly
            1,230 logical operations through the real transaction stack before
            crashing it. At that rate, seed sweeps run as a CI gate instead of
            a nightly job.
          </Lede>
          <P>
            The second is engine throughput against a production yardstick.
            SQLite is the honest choice of opponent for a single-node embedded
            engine, and the repository ships the comparison harness: identical
            workloads through <M>entropy::Database::execute</M> and{" "}
            <M>sqlite3_exec</M>, Google Benchmark timing both. Inserts run in
            non-durable mode on both sides (Entropy without fsync on commit,
            SQLite with <M>synchronous=OFF</M> and <M>journal_mode=MEMORY</M>),
            so the comparison measures engine work, not disk hardware. Point
            selects filter on a non-indexed column, so both engines scan.
          </P>
          <H2 n="6.1">The committed run: Apple M2</H2>
          <P>
            The repository&apos;s benchmark record (run file{" "}
            <M>bench-20251226-214051.json</M>, Apple clang 17, Release -O3)
            puts Entropy <Strong>ahead of SQLite at 0.90x on the 1k-row
            transactional insert batch</Strong> and behind at 1.39x on the
            10k batch. Point selects trail at 2.00x and 2.56x. For a young
            engine whose write path pays full WAL, MVCC versioning, and
            row-lock costs on every statement, holding a win on small
            transactional batches against SQLite is the result worth reporting,
            and the scan gap is the roadmap.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={148} />
          <Note label="method">
            Batch = one transaction of N single-row INSERTs; the per-op figure
            is the whole batch. Point select = one SELECT by equality on a
            non-indexed INTEGER column, re-run per iteration.
          </Note>
          <Note label="artifacts">
            Run files and a summary CSV live in docs/benchmarks/; scripts
            regenerate them with one command on any machine with SQLite3
            headers.
          </Note>
        </>
      }
    />
    <Figure
      n="6.1"
      caption={<>Per-op time, Entropy vs SQLite 3, from the committed M2 run. Insert panels are milliseconds per whole batch; select panels are microseconds per query. Ratios above 1.0 mean Entropy is slower.</>}
    >
      <BenchChart />
    </Figure>
  </Page>,

  <Page section={SECTION} key="ch6-2">
    <Cols
      main={
        <>
          <H2 n="6.2">A second machine, for honesty</H2>
          <P>
            The same binary rebuilt on a Debian 13 laptop (i7-1165G7, g++
            14.2.0, Release; SQLite 3.46.1) reproduces the shape and widens the
            gaps: mean of three repetitions puts the 1k insert batch at 1.49x,
            the 10k batch at 2.34x, and point selects at 3.47x and 4.44x. The
            conditions were deliberately unfavorable and are quoted with the
            numbers: CPU frequency scaling enabled and a background load
            average near 6.6 at capture, versus the quiet M2 run. The spread
            between the two machines is itself the finding: these are
            microbenchmarks of an engine without a benchmarking lab, and the
            committed run file is the number the repository stands behind.
          </P>
          <Tbl
            cols={["case", "entropy", "sqlite 3", "ratio"]}
            align="lrrr"
            fontSize={7.8}
            rows={[
              ["insert batch 1k (txn)", "3.58 ms", "2.40 ms", "1.49x"],
              ["insert batch 10k (txn)", "47.10 ms", "20.12 ms", "2.34x"],
              ["point select 1k", "151.5 µs", "43.7 µs", "3.47x"],
              ["point select 10k", "1.516 ms", "341.6 µs", "4.44x"],
            ]}
          />
          <H2 n="6.3">Where the time goes</H2>
          <P>
            Every Entropy query takes the whole path every time: lex, parse,
            bind, plan, then a freshly built volcano executor tree, with an
            MVCC snapshot and row locks underneath. There is no prepared-statement
            cache and no plan reuse. SQLite re-parses on each{" "}
            <M>sqlite3_exec</M> call too, but its parse-to-VM pipeline has had
            two decades of tuning. The 10k insert batch tells the same story
            from the write side: Entropy&apos;s per-row cost includes a WAL
            record, a version-chain entry, and a lock-table touch, and those
            costs compound linearly where SQLite&apos;s B-tree insert loop
            stays flat.
          </P>
          <P>
            The honest summary: on transactional insert batches at the 1k
            scale, Entropy is competitive with SQLite on one machine and
            within 1.5x on the other. On unindexed scans it pays 2x to 4.4x
            for a young executor. Nothing here is tuned yet; the cost model
            constants in chapter 04 are first principles, not profiles.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={30} />
          <Note label="linux capture">
            g++ (Debian 14.2.0-19), --benchmark_repetitions=3, mean real_time
            quoted. cpu_scaling_enabled=true and load_avg 6.6 in the Google
            Benchmark context header: treat as directional, not laboratory.
          </Note>
          <Note label="simulator throughput">
            time entropy-sim --schedule mixed --seeds 200: 0.39 s user, 0.398 s
            total, 99% cpu. All nine schedules at 200 seeds pass on this build:
            1,800 runs, 0 failures.
          </Note>
        </>
      }
    />
  </Page>,
];
