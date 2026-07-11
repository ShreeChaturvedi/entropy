import { Page } from "../components/Page";
import {
  ChapterOpen,
  Cols,
  P,
  Lede,
  H2,
  M,
  Strong,
  Note,
  Figure,
  Code,
  Tbl,
  Spacer,
} from "../components/Blocks";
import { SeedStreams } from "../figures/SeedStreams";
import { SimPipeline } from "../figures/SimPipeline";
import { C } from "../theme";

const SECTION = "01 · Deterministic simulation";

export const ch1Pages = [
  <Page section={SECTION} key="ch1-1">
    <ChapterOpen
      index="01"
      title="Crash recovery as a falsifiable claim"
      kicker="Deterministic simulation"
    />
    <Cols
      main={
        <>
          <Lede>
            Recovery carries the engine&apos;s hardest promise: after a crash at any
            instant, every committed row is still there, byte for byte, and no
            uncommitted write shows. Entropy tests that promise the way
            FoundationDB pioneered it: the real engine runs over simulated storage
            devices, every fault is drawn from a seeded generator, and a failing
            run replays exactly from one 64-bit integer.
          </Lede>
          <H2 n="1.1">Why determinism</H2>
          <P>
            A recovery bug lives in a window a few writes wide: the crash must land
            after the WAL buffer fills but before the fsync, or while half of a 4 KB
            page is on disk. Killing processes in a loop hits such windows by luck
            and cannot hit them again on demand, so the failure arrives without a
            reproduction and leaves without a regression test. The simulator inverts
            that economics. It decides when the crash happens and what the disk
            keeps, so the search over crash timings is systematic, and every failure
            is a permanent artifact: a seed.
          </P>
          <P>
            Nothing in the fault model reads a wall clock or an unseeded source.
            Two runs of the same seed under the same schedule produce equal fault
            logs, equal redo and undo counts, and an identical output line: the
            determinism a debugger needs and a CI gate can enforce.
          </P>
          <H2 n="1.2">One seed, three streams</H2>
          <P>
            <M>run_schedule</M> derives three independent PRNG streams from the
            run&apos;s seed with SplitMix64 mixing: salt <M>0x1</M> for the
            workload, <M>0x2</M> for the page device, <M>0x3</M> for the log store.
            Each derived value seeds its own <M>std::mt19937_64</M>, so workload
            decisions never perturb fault decisions and vice versa.
          </P>
          <P>
            Fault probabilities are integer parts-per-thousand compared against{" "}
            <M>rng() % 1000</M>, never floating point, so every threshold is exact
            and portable across platforms. A disabled fault never consumes a draw:
            switching one knob off cannot shift the sequence any other consumer
            sees.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={148} />
          <Note label="fault.hpp">
            derive_seed(seed, salt) applies the SplitMix64 finalizer:
            multiply-xor-shift mixing, stateless and deterministic. Independent
            streams from one seed, no draw-order interference.
          </Note>
          <Note label="scale">
            The schedule sweep in CI runs nine schedules across seeds 1 to 40: 360
            crash-recover-verify cycles on every push.
          </Note>
        </>
      }
    />
    <Spacer h={4} />
    <Figure n="1.1" caption={<>Seed derivation. One CLI-supplied integer fans out into three isolated Mersenne Twister streams; each simulated device and the workload own exactly one.</>}>
      <SeedStreams />
    </Figure>
  </Page>,

  <Page section={SECTION} key="ch1-2">
    <Cols
      main={
        <>
          <H2 n="1.3">The storage seam</H2>
          <P>
            The simulator swaps only the lowest layer. <M>SimDiskManager</M> and{" "}
            <M>SimLogStore</M> stand in for the file-backed page device and log
            store; everything above them (buffer pool, WAL manager, lock manager,
            MVCC version store, transaction manager, table heap) is the production
            code. The assembled stack keeps the engine&apos;s WAL-before-page steal
            hook, so the simulation exercises the same durability ordering the real
            engine enforces: no data page reaches disk before the log that
            describes it.
          </P>
          <P>
            A crash freezes the device and resolves the fate of every write that
            was issued but never fsynced. One roll per write (<M>draw_fate</M>)
            decides among three outcomes: the write is <Strong>lost</Strong>, the
            write is <Strong>torn</Strong>, or it happened to reach durable media{" "}
            <Strong>intact</Strong>. A torn page keeps a prefix or a suffix of the
            new bytes across a boundary drawn from [1, 4095], the other side
            reverting to the pre-crash image. A torn WAL tail keeps{" "}
            <M>rng() % (len+1)</M> bytes of the unsynced tail, and the cut lands
            mid-record, so the log reader&apos;s partial-record truncation is
            exercised end to end. Writes the engine fsynced are never touched:
            the model attacks exactly what a real power loss attacks.
          </P>
          <H2 n="1.4">Recovery under an oracle</H2>
          <P>
            While the workload runs, an oracle records the outcome each transaction
            reached: committed rows with their exact payloads, aborted and
            in-flight writes that must vanish. After the crash, the damaged images
            reopen on fresh, fault-free devices and{" "}
            <M>RecoveryManager::recover()</M> runs: the same analysis, redo, and
            undo the production engine runs at startup. Then{" "}
            <M>check_invariants</M> compares the recovered heap against the oracle:
          </P>
          <P>
            <M color={C.GREEN}>committed_present</M> requires every committed row
            to exist with a byte-identical payload.{" "}
            <M color={C.RED}>no_uncommitted_visible</M> requires that no in-flight
            or rolled-back write is visible. A non-ok status from recovery itself
            is recorded as <M>recovery_error</M>. Each run emits one line of JSONL
            with a stable schema; wall-clock recovery time is opt-in
            (<M>--timing</M>) precisely because it is the one non-reproducible
            field.
          </P>
        </>
      }
      rail={
        <>
          <Note label="tools/entropy-sim">
            The CLI sweeps seeds and prints one line per run. A sweep is
            machine-checkable output, not a green light: the JSONL stream is the
            artifact.
          </Note>
          <Spacer h={10} />
          <Code title="one run, one line" fontSize={7.3}>
{`{"seed":7,"schedule":"mixed",
 "faults_injected":13,
 "crash_point":
   "mixed_steal_lost_inflight",
 "outcome":"pass",
 "invariants_failed":[],
 "ops":57,"redo_ops":40,
 "undo_ops":0}`}
          </Code>
        </>
      }
    />
    <Figure n="1.2" caption={<>One run under run_schedule. The live stack runs on injecting devices; crash() applies the scheduled damage to unsynced state; recovery runs on clean devices and an oracle passes judgment.</>}>
      <SimPipeline />
    </Figure>
  </Page>,

  <Page section={SECTION} key="ch1-3">
    <Cols
      main={
        <>
          <H2 n="1.5">Named schedules</H2>
          <P>
            A schedule fixes everything except the seed: transaction count, whether
            a loser transaction is left in flight, whether dirty pages are stolen to
            disk before the crash, and the fault probabilities. Nine presets place
            the crash at the seams where recovery earns its keep: between the WAL
            flush and the page flush, mid-WAL-tail, after a steal of committed
            pages, and under transient write errors.
          </P>
        </>
      }
      rail={
        <Note label="sizing">
          A minimal insert&apos;s WAL record is ~79 bytes, so ~830 inserts overflow
          the 64 KiB WAL buffer. Schedules that need a genuinely unsynced tail run
          the in-flight loser for 1,200 inserts, forcing a mid-transaction overflow
          flush onto the device.
        </Note>
      }
    />
    <Tbl
      cols={["schedule", "crash point", "injected faults", "contract"]}
      align="llll"
      fontSize={7.6}
      rows={[
        [<M key="a">torn_wal_tail</M>, "in-flight txn, WAL tail at risk", "tail lost 50% · torn 50%", "both must fire"],
        [<M key="b">lost_page_write_after_commit</M>, "committed pages stolen, unsynced", "every page write lost", "loss must fire"],
        [<M key="c">crash_between_wal_and_page_flush</M>, "WAL durable, pages in flight", "every page write lost", "loss must fire"],
        [<M key="d">undo_durable_loser</M>, "loser's records durable, no COMMIT", "page writes lost", "undo_ops > 0, every seed"],
        [<M key="e">durable_survives_intact</M>, "clean fsync'd baseline", "none (control)", "zero faults, every seed"],
        [<M key="f">transient_write_errors</M>, "flush under failing writes", "30% IO errors + losses", "both must fire"],
        [<M key="g">mixed</M>, "steal + lost/torn WAL tail", "losses + tail lost 70% / torn 30%", "three kinds must fire"],
        [<M key="h" color={C.AMBER_BRIGHT}>live_abort_repro</M>, "aborts, then steal + loss", "page writes lost", "≥1 abort, every seed"],
        [<M key="i" color={C.AMBER_BRIGHT}>torn_page_write</M>, "committed pages torn, unsynced", "every page write torn", "tear must fire"],
      ]}
    />
    <Cols
      style={{ marginTop: 8 }}
      main={
        <>
          <H2 n="1.6">Contracts against vacuity</H2>
          <P>
            A fault-injection suite has a quiet failure mode of its own: the
            advertised fault stops firing (a refactor moves an fsync, a buffer
            stops overflowing) and the suite stays green while testing nothing.
            Every schedule therefore carries a contract the sweep asserts alongside
            the invariants. <M>must_fire</M> lists fault kinds that must occur at
            least once across the sweep&apos;s seeds; <M>expect_undo</M> requires
            recovery&apos;s undo phase to do real work on every seed;{" "}
            <M>expect_aborts</M> requires at least one live abort per seed;{" "}
            <M>expect_zero_faults</M> pins the clean-baseline control at exactly
            zero injected faults.
          </P>
          <P>
            The checker itself is under test. A tenth schedule,{" "}
            <M>skip_recovery</M>, repeats <M>lost_page_write_after_commit</M> but
            skips recovery. Committed rows then exist only in the WAL, and the
            sweep requires the invariant checker to report the loss; a checker
            that cannot detect a real loss would prove nothing by staying green.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={2} />
          <Note label="highlighted rows">
            The two amber schedules are regression locks for real bugs the
            simulator caught. Chapter 02 tells both stories.
          </Note>
        </>
      }
    />
    <Code title="sweep summaries · this build, 200 seeds per schedule, verbatim" fontSize={7.6}>
{`entropy-sim: schedule=torn_wal_tail seeds=200 start=1  200 passed, 0 failed, 0 errored
faults fired: lost_wal_tail=94 torn_wal_tail=106  undo_exercised_runs=106

entropy-sim: schedule=undo_durable_loser seeds=200 start=1  200 passed, 0 failed, 0 errored
faults fired: lost_page_write=2200  undo_exercised_runs=200

entropy-sim: schedule=mixed seeds=200 start=1  200 passed, 0 failed, 0 errored
faults fired: lost_page_write=200 lost_wal_tail=139 torn_wal_tail=61  undo_exercised_runs=61

entropy-sim: schedule=durable_survives_intact seeds=200 start=1  200 passed, 0 failed, 0 errored
faults fired: none  undo_exercised_runs=0`}
    </Code>
  </Page>,
];
