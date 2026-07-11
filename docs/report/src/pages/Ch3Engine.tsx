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
  Spacer,
} from "../components/Blocks";
import { PageLayout } from "../figures/PageLayout";
import { LatchCrabbing } from "../figures/LatchCrabbing";
import { MvccTimeline } from "../figures/MvccTimeline";
import { RecoveryPhases } from "../figures/RecoveryPhases";

const SECTION = "03 · The engine";

export const ch3Pages = [
  <Page section={SECTION} key="ch3-1">
    <ChapterOpen index="03" title="The engine under test" kicker="Storage and transactions" />
    <Cols
      main={
        <>
          <Lede>
            The simulator is only as interesting as the machine it attacks.
            Entropy is a relational engine written from scratch in C++20, about
            22,500 non-empty lines of source under a 12,700-line test suite,
            with no third-party code on any hot path: the parser, the B+ tree,
            the buffer pool, the WAL, and recovery are all first-party. This
            chapter follows the write path from the page format up through the
            transaction stack, the same path the schedules in chapter 01 crash.
          </Lede>
          <H2 n="3.1">Slotted pages</H2>
          <P>
            Every durable structure lives on 4096-byte pages that begin with the
            same 32-byte header, laid out to be free of compiler padding and
            static-asserted at that size. A table page manages its interior as a
            classic slotted page: a slot array grows down from the header, four
            bytes per slot (offset and length), while record bytes grow up from
            the end. The two free-space fields in the header bound the
            contiguous gap between them.
          </P>
          <P>
            Deleting a record clears its slot but leaves the bytes; a{" "}
            <M>compact()</M> pass reclaims the space when an insert would
            otherwise fail. Updates write in place when the new payload fits the
            old slot and relocate to fresh space when it does not. Slot indices
            are identity: a record&apos;s RID (page id, slot) survives in-place
            updates, and a slot freed by an uncommitted delete is held in
            reserve, not reused, until the deleter&apos;s fate is known.
          </P>
          <P>
            The header&apos;s checksum field is the torn-write detector from
            chapter 02: a CRC-32 over the full page image with its own four
            bytes carved out, stamped on write, verified on every read, on by
            default in both the file-backed and simulated disk managers.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={148} />
          <Note label="tuples">
            Rows serialize as a null bitmap, fixed-width columns in schema
            order, then length-prefixed varlen data. VARCHAR carries a 16-bit
            length; a page-sized cap keeps any tuple under 4 KB by
            configuration.
          </Note>
          <Note label="buffer pool">
            1,024 frames by default (4 MiB). Pin counts guard reuse, an LRU list
            orders eviction, and a dirty victim is flushed through a hook that
            first forces the WAL to the page&apos;s LSN: steal is allowed, but
            no page may precede its log to disk.
          </Note>
        </>
      }
    />
    <Figure
      n="3.1"
      caption={<>The on-disk unit. Slots grow toward the records and records toward the slots; the header's checksum makes the whole 4 KB image self-verifying.</>}
    >
      <PageLayout />
    </Figure>
  </Page>,

  <Page section={SECTION} key="ch3-2">
    <Cols
      main={
        <>
          <H2 n="3.2">B+ tree, crabbed</H2>
          <P>
            The primary index is a B+ tree over 64-bit integer keys mapping to
            RIDs. Node capacity comes from the page arithmetic, not a tuning
            knob: after the shared header, a leaf holds up to 252 key-RID pairs
            and an internal node up to 337 separators. Leaves link both ways, so
            range scans ride sibling pointers without touching the tree above.
          </P>
          <P>
            Concurrency is hand-over-hand latching on the pages themselves.
            Readers descend with shared latches, releasing each parent the
            moment the child is held, and hold at most two pages at any instant.
            Structural writers serialize on a single mutex, then crab down with
            exclusive latches, shedding every ancestor above the deepest node
            that cannot split or underflow. What a writer retains is exactly the
            path a restructure can touch, so a reader crossing a split either
            passes the parent before the change and finds the old child, or
            after it and finds the routed sibling. Never a half-made tree.
          </P>
          <P>
            The deadlock argument is the part worth reading twice, and it lives
            as a 70-line comment in <M>b_plus_tree.cpp</M>. At the leaf level,
            every latch wait runs left to right; a writer that must turn left to
            borrow from a sibling first releases any right-hand latch it holds,
            because re-latching a leaf while holding one to its right can
            deadlock against a rightward chain scan. That exact interleaving
            surfaced as a live deadlock in review, and the release rule is its
            fix. Parent-pointer maintenance takes no latches at all: the field
            is written and read only under the writer mutex. And a grown root is
            published unlatched, through a release-store of the root page id
            that readers observe with an acquire load.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={30} />
          <Note label="frame reuse">
            The latch lives in the frame object, not the page bytes, and a
            latched page stays pinned, so the buffer pool can never evict and
            reuse a frame underneath a latch holder.
          </Note>
          <Note label="internal levels">
            The writer&apos;s internal-level relatch order is not globally
            consistent, and does not need to be: the underflowing node&apos;s
            parent stays write-latched throughout, fencing every descending
            reader out of the subtree. A latch nobody else can request cannot
            join a cycle.
          </Note>
        </>
      }
    />
    <Figure
      n="3.2"
      caption={<>Latch crabbing. The writer holds its restructure path and nothing above; readers hold parent-child pairs on the way down and leaf pairs across scans, always waiting rightward.</>}
    >
      <LatchCrabbing />
    </Figure>
  </Page>,

  <Page section={SECTION} key="ch3-3">
    <Cols
      main={
        <>
          <H2 n="3.3">MVCC snapshot isolation</H2>
          <P>
            Transactions read through snapshots. A single atomic counter issues
            timestamps; a transaction&apos;s <M>start_ts</M> fixes its view of
            the world, and each RID carries a chain of versions stamped with{" "}
            <M>begin_ts</M> and <M>end_ts</M>. A version is visible when it was
            created at or before the snapshot and not yet deleted from its
            view; a transaction&apos;s own uncommitted writes are always
            visible to itself. Readers never block writers and writers never
            block readers, because reading consults the chain, not a lock.
          </P>
          <P>
            Write-write conflicts resolve first-updater-wins. A second writer
            touching the same RID aborts on any of four conditions: another
            transaction&apos;s uncommitted create, another&apos;s uncommitted
            delete, a committed delete of the current version, or a committed
            create newer than the writer&apos;s snapshot. The conflict check
            sits in the version store, so it fires at write time, not commit
            time, and the loser aborts early instead of doing doomed work.
          </P>
          <P>
            Row and table locks still exist for writes, managed with FIFO
            queues, shared and exclusive modes, and upgrades. Deadlocks are
            detected in a wait-for graph and broken wait-die: the older
            requester wins and the younger transaction aborts, with a five
            second timeout as the backstop. Rollback walks the write set in
            reverse and, per chapter 02, logs one compensation record per
            undone write before the ABORT record appends.
          </P>
          <H2 n="3.4">Commit, in order</H2>
          <P>
            Commit is an ordering exercise. The transaction appends its COMMIT
            record, forces the WAL through that record&apos;s LSN with an
            fsync, and only then stamps its commit timestamp into its versions,
            making them visible to later snapshots. Locks release after
            visibility, version chains prune once no live snapshot can reach
            the old entries, and the transaction object retires. Nothing about
            durability waits on data pages: they follow lazily through the
            buffer pool, which is exactly the window the chapter 01 schedules
            crash into, and the WAL closes it.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={30} />
          <Note label="version store">
            Version chains are an in-memory structure rebuilt fresh after a
            crash; durability belongs to the WAL and the heap. VersionInfo is
            32 bytes: created_by, deleted_by, begin_ts, end_ts.
          </Note>
          <Note label="gc">
            Commit finalizes a transaction&apos;s versions and prunes chains no
            live snapshot can reach, so long chains decay as old readers
            retire.
          </Note>
        </>
      }
    />
    <Figure
      n="3.3"
      caption={<>Visibility of one RID across two snapshots. T_a, started between v1's creation and its deletion, keeps reading v1 even after v2 commits; T_b, started later, reads v2.</>}
    >
      <MvccTimeline />
    </Figure>
  </Page>,

  <Page section={SECTION} key="ch3-4">
    <Cols
      main={
        <>
          <H2 n="3.5">Write-ahead log</H2>
          <P>
            Every mutation appends a log record before its page changes: a
            32-byte header (LSN, transaction id, previous-LSN chain, payload
            size, type) followed by a typed payload, with before-images on
            updates and deletes so undo has something to restore. Records
            buffer in 64 KiB of user space; a commit forces the buffer through{" "}
            <M>flush_to_lsn</M> up to its own commit record and fsyncs the log
            device before the commit returns. The reader side tolerates a torn
            tail by construction: a record that fails its bounds check
            truncates the log at the last whole record, which is exactly the
            behavior the <M>torn_wal_tail</M> schedule sweeps.
          </P>
          <H2 n="3.6">Recovery in three passes</H2>
          <P>
            Recovery is ARIES reduced to what a single-node engine needs.
            Analysis scans forward from the last checkpoint, rebuilding the set
            of transactions with no durable outcome and fixing the LSN where
            redo must begin. Redo repeats history for every data record,
            committed or not, gated per page: a page whose LSN is at or past
            the record&apos;s already reflects it, so replay skips. The gate is
            what makes redo idempotent, and recovery safe to crash and re-run.
            Undo then walks the losers&apos; records newest-first and applies
            state-checked inverses, flushing compensated pages before the
            closing ABORT records append, the same
            compensation-before-outcome ordering the live abort path follows.
          </P>
          <P>
            Checkpoints bound the work: flush the WAL, flush dirty pages,
            append a checkpoint record naming the active transactions, and
            advance the redo anchor. The engine takes them at startup and after{" "}
            <M>DROP TABLE</M> rather than on a timer, a real bound on recovery
            time that chapter 07 lists with the other honest edges. Catalog
            durability rides a separate manifest file, replaced atomically:
            write to a temp file, fsync it, rename into place, fsync the
            directory.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={30} />
          <Note label="page-lsn discipline">
            Redo stamps a page&apos;s LSN when it applies a record, and the
            forward path stamps compensation LSNs at abort time. The pairing
            keeps the gate honest whether a page survived the crash or was
            rebuilt from zero.
          </Note>
          <Note label="record types">
            BEGIN, COMMIT, ABORT; INSERT, DELETE, UPDATE with before-images;
            CHECKPOINT and END_CHECKPOINT. LSNs are 64-bit and start at 1; 0 is
            the invalid sentinel a fresh page carries.
          </Note>
        </>
      }
    />
    <Figure
      n="3.4"
      caption={<>Three passes over one log. Analysis finds t9 with no outcome record; redo replays both transactions' writes; undo erases t9's, in reverse, with compensation ordering preserved.</>}
    >
      <RecoveryPhases />
    </Figure>
  </Page>,
];
