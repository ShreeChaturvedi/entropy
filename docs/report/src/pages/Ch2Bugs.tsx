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
  Spacer,
  Tbl,
} from "../components/Blocks";
import { AbortResurrection } from "../figures/AbortResurrection";
import { TornPageAnatomy } from "../figures/TornPageAnatomy";
import { SweepBars } from "../figures/SweepBars";
import { C } from "../theme";

const SECTION = "02 · Two bugs";

export const ch2Pages = [
  <Page section={SECTION} key="ch2-1">
    <ChapterOpen
      index="02"
      title="Two bugs the test suite never caught"
      kicker="Case studies"
    />
    <Cols
      main={
        <>
          <Lede>
            The engine came first, by about seven months. Storage, the B+ tree,
            MVCC, the WAL, and recovery each carried unit and integration suites,
            more than 550 tests passing, when the simulator ran its first wide
            seed sweeps. Those sweeps found two bugs in the crash path of that
            tested code. Both violated atomicity or durability. Both were
            invisible to example-based tests, because both lived in states no
            hand-written test thought to construct.
          </Lede>
          <P>
            Each case followed the same arc. A schedule failed on specific seeds.
            The seed replayed the failure deterministically until the root cause
            fell out. A fix landed. The schedule that found the bug joined the
            permanent sweep, where it re-runs the scenario across 40 seeds on
            every CI push. The failure, the fix, and the guarantee are all in the
            record: issues #75, #81, and #86, and the fix commits{" "}
            <M>eebf21c</M> and <M>8bd6db2</M>.
          </P>
          <H2 n="2.1">Case I · the resurrected abort</H2>
          <P>
            The <M>live_abort_repro</M> schedule lets transactions abort during
            normal operation (400‰ per transaction), commits the rest, steals the
            committed pages to disk unsynced, and crashes so every one of those
            page writes is lost. Recovery must rebuild the table from the WAL
            alone. Under the pre-fix engine the sweep failed on{" "}
            <Strong>every seed: 40 of 40</Strong>, reporting{" "}
            <M color={C.RED}>no_uncommitted_visible</M> (an aborted row came
            back) and, on seeds where a later transaction had reused the aborted
            row&apos;s slot, <M color={C.RED}>committed_present</M> (the
            committed row was gone too).
          </P>
          <P>
            The failure shrinks to three WAL records. A transaction begins,
            inserts a row, and aborts, all cleanly, no crash yet. The WAL now
            holds <M>BEGIN</M>, <M>INSERT</M>, <M>ABORT</M>. Recover that log
            onto a fresh buffer pool and the aborted row is present:
          </P>
          <Code title="issue #81 · minimal reproduction" fontSize={8}>
{`// WAL: BEGIN(1), INSERT(1, rid=(0,0), data), ABORT(1) — flushed, durable.
RecoveryManager rec(pool, wal, disk);
rec.recover();          // ok=1, redo_count=1, undo_count=0
// RID(0,0) is PRESENT after recovery — the aborted insert was resurrected.`}
          </Code>
          <P>
            Two failure signatures, one mechanism. Which one a seed reports
            depends only on whether a later committed transaction happened to
            reuse the slot the abort freed:
          </P>
          <Tbl
            cols={["invariant violated", "observable damage", "when"]}
            align="lll"
            fontSize={7.8}
            rows={[
              [
                <M key="a" color={C.RED}>no_uncommitted_visible</M>,
                "an aborted row is visible after recovery",
                "every failing seed",
              ],
              [
                <M key="b" color={C.RED}>committed_present</M>,
                "a committed row is missing outright",
                "seeds with slot reuse",
              ],
            ]}
          />
          <P>
            The failing schedule landed with the simulator itself, deliberately
            excluded from the passing sweep and annotated with the open issue
            numbers, so the failure survived as an executable artifact rather
            than as prose in a tracker. Promotion into the permanent sweep came
            later, inside the same pull request as the fix, which is what makes
            the schedule a regression lock rather than a demonstration.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={128} />
          <Note label="why tests missed it">
            The recovery unit tests hand-stamped page LSNs on their crafted
            pages, simulating maintenance the forward write path did not actually
            perform. The simulator assembled its pages the way production does,
            and the gap surfaced.
          </Note>
          <Note label="reproduce">
            entropy-sim --schedule live_abort_repro --seeds 40. Before the fix:
            40/40 fail. After: 40/40 pass, aborts firing on every seed.
          </Note>
        </>
      }
    />
  </Page>,

  <Page section={SECTION} key="ch2-2">
    <Cols
      main={
        <>
          <H2 n="2.2">Root cause: repeat history, with nothing to reverse</H2>
          <P>
            ARIES-style recovery repeats history: the redo phase replays every
            logged data change, then the undo phase rolls back losers,
            transactions with no durable outcome record. A cleanly aborted
            transaction is not a loser. Its <M>ABORT</M> record is in the log, so
            analysis strikes it from the active-transaction table and undo never
            touches it. That is correct, but it rests on an assumption: the log
            must also carry the abort&apos;s undo work, so that repeating history
            repeats the compensation too.
          </P>
          <P>
            Entropy&apos;s forward abort path broke that assumption. It undid the
            transaction&apos;s writes physically, in memory, flushed the
            compensated pages, and appended <M>ABORT</M>, logging no
            compensation records at all. As long as the compensated pages
            survived, nothing was wrong. The simulator&apos;s contribution was to
            take them away: steal the pages unsynced, lose them at the crash, and
            recovery rebuilds from a WAL that says <M>INSERT</M> and never says
            anything else. Page-LSN gating cannot save the rebuilt page either;
            a fresh frame carries LSN 0, so the gate never fires.
          </P>
          <P>
            The slot-reuse variant turned an atomicity bug into data loss. After
            a live abort freed the row&apos;s slot, a later committed transaction
            reused it. During recovery, redo of the resurrected aborted insert
            occupied the slot first, and redo of the committed insert then found
            it taken and gave up: the aborted row present, the committed row
            gone.
          </P>
          <H2 n="2.3">Fix and guarantee</H2>
          <P>
            Commit <M>eebf21c</M> makes the abort path speak up in the log:
            one redoable compensation record (CLR) per undone write, stamped
            with the page&apos;s LSN and durable before the <M>ABORT</M> record
            appends. An undone insert logs a delete at the same RID, an undone
            delete logs an insert of the before-image, an undone update logs an
            update back to the before-image. Redo now replays each aborted
            mutation and its inverse, so the transaction leaves nothing behind
            even when its compensated pages never reached disk.
          </P>
          <P>
            The schedule that found the bug became its regression lock.{" "}
            <M>live_abort_repro</M> joined the permanent sweep with{" "}
            <M>expect_aborts</M> asserted, so the coverage cannot silently decay:
            if aborts ever stop firing, the sweep fails on that alone. After the
            fix, 40 of 40 seeds pass in CI and a wider 500-seed sweep recorded
            500 of 500.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={30} />
          <Note label="issue #75">
            Filed alongside: the forward write path never stamped page LSNs, so
            redo was never gated for forward-written pages. The CLR fix closes
            the abort hole; LSN stamping keeps redo idempotent when pages do
            survive.
          </Note>
          <Note label="ordering">
            The engine already made compensation durable before appending ABORT.
            The fix preserves that ordering for the CLRs, so a crash between the
            two leaves a loser, which undo already handles.
          </Note>
        </>
      }
    />
    <Figure
      n="2.1"
      caption={<>The resurrected abort. Before the fix, the durable log holds the aborted INSERT and no record of its undo; redo replays it and undo skips the transaction. After eebf21c, a compensation record rides in the log and repeat-history redo removes what it re-inserts.</>}
    >
      <AbortResurrection />
    </Figure>
  </Page>,

  <Page section={SECTION} key="ch2-3">
    <Cols
      main={
        <>
          <H2 n="2.4">Case II · the torn page that read back clean</H2>
          <P>
            A torn write is a crash persisting only part of a page write, a
            standard failure mode whenever the page size exceeds the
            device&apos;s atomic write unit. The <M>torn_page_write</M> schedule
            manufactures exactly that: commit fourteen transactions, steal the
            pages unsynced, and tear every unsynced page write at the crash,
            keeping a prefix or a suffix across a random boundary. Recovery then
            faces a disk full of half-old, half-new pages next to a fully
            durable WAL.
          </P>
          <P>
            The pre-fix sweep failed <M color={C.RED}>committed_present</M> on{" "}
            <Strong>95 of 200 seeds</Strong>, and the split was not noise. Which
            side of the boundary survived decided the outcome. When the tear
            zeroed the header, redo saw an uninitialized page, re-created it,
            and replayed everything: those seeds passed. When the tear kept the
            first 128 bytes, the header and the slot directory, the page passed
            every check the engine had while its record region held zeros.
          </P>
          <P>
            That geometry defeated recovery three ways at once. The surviving
            header still said <M>TABLE_PAGE</M>, so redo did not re-initialize
            the page. The slot directory still claimed the record was live, so
            redo&apos;s <M>insert_record_at</M> refused to write into an
            &quot;occupied&quot; slot. And nothing else validated the page, so{" "}
            <M>recover()</M> returned success with <M>redo=0, undo=0</M> while a
            committed row&apos;s bytes were zeros. Silent loss, signed off as a
            clean recovery.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={30} />
          <Note label="the record">
            Issue #86, with the failing sweep attached. A wider pre-fix sweep
            recorded ~220 failing seeds of 500. Reproduce the fixed behavior:
            entropy-sim --schedule torn_page_write --seeds 200.
          </Note>
          <Note label="why tests missed it">
            Every hand-written recovery test wrote whole pages. Only fault
            injection at the device seam produced a page that was
            half-persisted, and only the invariant checker noticed that a
            &quot;successful&quot; recovery had served zeros.
          </Note>
        </>
      }
    />
    <Figure
      n="2.2"
      caption={<>Anatomy of the fatal tear. The prefix that survives is precisely the part that convinces recovery the page is healthy: a valid header and a slot directory pointing at zeroed bytes.</>}
    >
      <TornPageAnatomy />
    </Figure>
  </Page>,

  <Page section={SECTION} key="ch2-4">
    <Cols
      main={
        <>
          <H2 n="2.5">Fix: make the page carry proof of itself</H2>
          <P>
            Commit <M>8bd6db2</M> gives every page an integrity stamp: a CRC-32
            (IEEE polynomial <M>0xEDB88320</M>) over the full 4096-byte image,
            stored in header bytes [12,16) and carved out of its own
            computation. The disk manager stamps on write and verifies on read.
            A torn page no longer matches its stamp, so the read returns{" "}
            <M>Status::Corruption</M> instead of serving half-persisted bytes as
            truth. An all-zero page verifies clean by rule, so freshly extended
            files need no stamp.
          </P>
          <P>
            Detection alone would only turn silent loss into a loud error, so
            the buffer pool gained a recovery-mode seam. During recovery, a read
            that fails its checksum reinitializes the frame to a fresh, empty
            page with an invalid LSN, and repeat-history redo rebuilds every
            committed row onto it from the WAL. Outside recovery the same read
            stays a hard error. The fatal geometry from the sweep, header and
            slot directory intact over zeroed records, now takes the same path
            as a zeroed header: distrust the fragment, rebuild from the log.
          </P>
          <P>
            The file-backed engine shipped the checksum default-off at first, on
            the belief that byte offset 12 collided with the B+ tree&apos;s{" "}
            <M>parent_page_id</M>. It does not: B+ tree nodes place their own
            header at offset 32, past the common page header, which puts{" "}
            <M>parent_page_id</M> at offset 44. Commit <M>62800ef</M> corrected
            the analysis and enabled checksums by default, so the real engine
            now reports torn writes and bit rot as corruption on every read
            path, not only under simulation.
          </P>
          <H2 n="2.6">What the two cases share</H2>
          <P>
            Neither bug was exotic. One was a missing log record on a code path
            every database has; the other was a missing integrity check on a
            failure mode every storage engine faces. Both sat under a green
            suite of 550-plus tests, in the gap between the states tests
            construct and the states crashes construct. The schedules that
            exposed them now run as permanent locks, 80 crash-recover-verify
            cycles between them on every push, each with a contract that fails
            the sweep if its fault ever stops firing.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={30} />
          <Note label="scope">
            The CRC detects corruption; it does not repair it. Repair comes from
            the WAL, which is why the checksum ships with the recovery-mode
            rebuild rather than alone.
          </Note>
          <Note label="alternatives">
            Full-page-write journaling (InnoDB&apos;s doublewrite, Postgres
            FPIs) repairs torn pages without a rebuild but pays for it on every
            checkpoint. For an engine whose WAL already replays whole histories,
            detect-and-redo is the cheaper contract.
          </Note>
        </>
      }
    />
    <Figure
      n="2.3"
      caption={<>Recorded sweeps around each fix. live_abort_repro failed all 40 seeds before eebf21c; torn_page_write failed 95 of 200 before 8bd6db2. Both now pass their full sweeps, re-verified on every CI run across seeds 1 to 40.</>}
    >
      <SweepBars />
    </Figure>
  </Page>,
];
