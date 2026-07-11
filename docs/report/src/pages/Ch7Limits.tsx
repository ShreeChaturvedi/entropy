import { Page } from "../components/Page";
import {
  ChapterOpen,
  Cols,
  P,
  Lede,
  H2,
  M,
  Note,
  Code,
  Spacer,
} from "../components/Blocks";

const SECTION = "07 · Limitations";

export const ch7Pages = [
  <Page section={SECTION} key="ch7-1">
    <ChapterOpen index="07" title="What this engine does not do" kicker="Known limits, verified in code" />
    <Cols
      main={
        <>
          <Lede>
            Every claim in this document was checked against the source before
            it was printed, and the same standard applies in reverse: these are
            the places the engine falls short today, each verified in code and
            most tracked as open issues.
          </Lede>
          <H2 n="7.1">Secondary indexes desynchronize on writes</H2>
          <P>
            The DML executors touch only the table heap: <M>INSERT</M>,{" "}
            <M>UPDATE</M>, and <M>DELETE</M> never update secondary indexes, so
            an index built before a write is stale after it (issue #11). The
            B+ tree also rejects duplicate keys, so building an index over a
            non-unique column silently drops rows and an equality lookup
            returns at most one match (issue #8). The heap is authoritative and
            sequential scans, the transactional engine, and everything the
            simulator tests are unaffected, but index-accelerated reads are
            only trustworthy on unique keys over data that has not changed
            since the build.
          </P>
          <H2 n="7.2">Index DDL parses but does not execute</H2>
          <P>
            The parser accepts <M>CREATE INDEX</M> and <M>DROP INDEX</M>, and
            the catalog supports both operations through the C++ API, but{" "}
            <M>Database::execute</M> rejects the statement types (issue #35).
            The same issue records that DECIMAL, TIMESTAMP, DATE, and CHAR
            columns cannot be declared from SQL text, and that the lexer
            accepts unterminated string literals and block comments instead of
            raising errors. Aggregates, by contrast, do run from SQL: SUM,
            AVG, COUNT, MIN, MAX, and GROUP BY execute through the hash
            aggregation path.
          </P>
          <H2 n="7.3">Durability edges</H2>
          <P>
            Checkpoints happen at startup and after <M>DROP TABLE</M>, not on
            a timer, so a long-running instance accumulates unbounded redo
            work between checkpoints. Data-page <M>sync()</M> in the
            file-backed disk manager flushes the stream without an fsync;
            durability rests on the WAL, which does fsync, and on redo, which
            rebuilds pages from it. The page free list is in-memory only, so
            pages freed before a restart are leaked until a durable free map
            exists. Version chains are also memory-resident: a restart
            recovers committed data exactly, but as single versions, not
            history.
          </P>
          <H2 n="7.4">Scope</H2>
          <P>
            Single-node, embedded, local-only by design: no replication, no
            network protocol, no subqueries, CTEs, or window functions. The
            simulator models faults at the storage seam (lost, torn, and
            erroring writes) and does not inject in-page bit flips; the
            checksums of chapter 02 cover that class on the read path instead.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={128} />
          <Code title="verified live, this build" fontSize={7.3}>
{`entropy> CREATE INDEX idx
    ...>   ON t (id);
Error: NotSupported:
  Unsupported statement type`}
          </Code>
          <Note label="why print this">
            A report that overstates one capability forfeits the rest. The
            engine&apos;s claims stand on the crash record of chapters 01 and
            02; the gaps above are the work that remains, not footnotes to
            hide.
          </Note>
        </>
      }
    />
  </Page>,
];
