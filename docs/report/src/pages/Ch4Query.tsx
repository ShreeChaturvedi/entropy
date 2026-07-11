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
  Tbl,
  Spacer,
} from "../components/Blocks";

const SECTION = "04 · Query path";

export const ch4Pages = [
  <Page section={SECTION} key="ch4-1">
    <ChapterOpen index="04" title="From SQL text to a costed plan" kicker="Parser, optimizer, executors" />
    <Cols
      main={
        <>
          <Lede>
            The query layer is deliberately conventional: a hand-written
            recursive-descent parser, a binder that fails bad types before
            execution, a Selinger-style cost model with explicit constants, and
            volcano-model executors. No parser generator, no expression JIT,
            nothing on the path that is not first-party code.
          </Lede>
          <H2 n="4.1">Parse, then bind</H2>
          <P>
            The parser covers SELECT with joins (inner, left, right, cross),
            WHERE, GROUP BY with the standard aggregates, ORDER BY, and
            LIMIT/OFFSET, plus INSERT, UPDATE, DELETE, CREATE/DROP TABLE, and
            EXPLAIN, descending by precedence from OR down to unary operators.
            The binder then resolves every column against the catalog and
            type-checks literals against the schema, so an ill-typed INSERT or
            a misspelled column dies at bind time with a named error, not at
            execution depth.
          </P>
          <H2 n="4.2">Costing with the constants on the table</H2>
          <P>
            The cost model prices plans in explicit units rather than folklore.
            A sequential scan costs its page count plus a per-tuple CPU term; an
            index scan pays a logarithmic descent plus one random page fetch
            per match, at four times the sequential page price, which is what
            makes it lose on low-selectivity predicates. Selectivity defaults
            follow Selinger: one tenth for equality, one third for a range,
            refined by row counts and min/max when statistics exist. Joins are
            chosen the same way: an equi-join takes the hash path when build
            plus probe undercuts the nested-loop product, and keeps nested loop
            otherwise, since it works for any predicate.
          </P>
          <Tbl
            cols={["constant", "value", "meaning"]}
            align="lrl"
            fontSize={7.8}
            rows={[
              [<M key="a">SEQ_PAGE_COST</M>, "1.0", "sequential page read (the unit)"],
              [<M key="b">RANDOM_PAGE_COST</M>, "4.0", "random page read (index fetch)"],
              [<M key="c">TUPLE_CPU_COST</M>, "0.01", "examine one tuple"],
              [<M key="d">INDEX_TUPLE_COST</M>, "0.005", "touch one index entry"],
              [<M key="e">HASH_QUAL_COST</M>, "0.025", "probe one hash bucket"],
              [<M key="f">HASH_BUILD_COST</M>, "0.05", "insert one build row"],
              [<M key="g">SORT_COST</M>, "0.02", "per comparison, n·log n"],
            ]}
          />
          <H2 n="4.3">Volcano execution</H2>
          <P>
            Plans run as a tree of iterators with <M>init()</M> and{" "}
            <M>next()</M>: scans (sequential and index), filter, projection,
            nested-loop and hash join, hash aggregation, sort, limit, and the
            three DML executors. One tuple flows at a time, so memory stays
            bounded by the pipeline&apos;s blocking operators (sort and the hash
            build), and every operator composes with every other.
          </P>
        </>
      }
      rail={
        <>
          <Spacer h={100} />
          <Code title="EXPLAIN, verbatim from the shell" fontSize={7.3}>
{`entropy> EXPLAIN SELECT name
    ...>   FROM users
    ...>  WHERE age > 35
    ...>  ORDER BY age DESC
    ...>  LIMIT 10;
+-----------------------------+
| QUERY PLAN                  |
+-----------------------------+
| Query Plan:                 |
| -> Sequential Scan on users |
|    SeqScan Cost: 1.020000   |
|    Filter: (predicate)      |
| -> Sort                     |
|    Key: age DESC            |
| -> Limit: 10                |
| Estimated Rows: 0           |
+-----------------------------+`}
          </Code>
          <Note label="reading the cost">
            Captured after two INSERTs: 1.02 = one page + 2 rows x 0.01
            examined. Small numbers, but the same arithmetic ranks plans at any
            scale.
          </Note>
          <Note label="hash aggregation">
            GROUP BY accumulates FNV-1a-hashed group keys in one pass, with
            SUM(INTEGER) promoted to 64-bit to dodge overflow, and AVG divided
            at output time.
          </Note>
        </>
      }
    />
  </Page>,
];
