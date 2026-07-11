import type { CSSProperties, ReactNode } from "react";
import { C, F, T } from "../theme";

/* ---------- layout ---------- */

/** Main text column + outer rail for notes/captions. */
export function Cols({
  main,
  rail,
  style,
}: {
  main: ReactNode;
  rail?: ReactNode;
  style?: CSSProperties;
}) {
  return (
    <div style={{ display: "flex", gap: 30, alignItems: "stretch", ...style }}>
      <div style={{ width: 438, flex: "0 0 438px", minWidth: 0 }}>{main}</div>
      <div style={{ flex: 1, minWidth: 0 }}>{rail}</div>
    </div>
  );
}

export function Spacer({ h }: { h: number }) {
  return <div style={{ height: h }} />;
}

/* ---------- headings ---------- */

export function Eyebrow({ children, color = C.AMBER }: { children: ReactNode; color?: string }) {
  return <div style={{ ...T.eyebrow, color }}>{children}</div>;
}

export function ChapterOpen({
  index,
  title,
  kicker,
}: {
  index: string;
  title: ReactNode;
  kicker?: ReactNode;
}) {
  return (
    <header style={{ marginBottom: 26 }}>
      <div style={{ display: "flex", alignItems: "baseline", gap: 16, marginBottom: 10 }}>
        <span
          style={{
            fontFamily: F.SERIF,
            fontStyle: "italic",
            fontSize: 30,
            color: C.AMBER,
            lineHeight: 1,
          }}
        >
          {index}
        </span>
        <span style={{ flex: 1, borderTop: `0.5px solid ${C.HAIRLINE}`, transform: "translateY(-9px)" }} />
        {kicker && <Eyebrow color={C.INK_SUBTLE}>{kicker}</Eyebrow>}
      </div>
      <h1 style={{ ...T.chapterTitle, margin: 0, maxWidth: 560 }}>{title}</h1>
    </header>
  );
}

export function H2({ n, children }: { n?: string; children: ReactNode }) {
  return (
    <h2 style={{ ...T.h2, margin: "20px 0 8px", display: "flex", gap: 10, alignItems: "baseline" }}>
      {n && (
        <span style={{ fontFamily: F.MONO, fontSize: 8.6, fontWeight: 500, color: C.AMBER, letterSpacing: "0.06em" }}>
          {n}
        </span>
      )}
      <span>{children}</span>
    </h2>
  );
}

export function H3({ children }: { children: ReactNode }) {
  return <h3 style={{ ...T.h3, margin: "14px 0 5px" }}>{children}</h3>;
}

/* ---------- prose ---------- */

export function P({ children, style }: { children: ReactNode; style?: CSSProperties }) {
  return (
    <p
      lang="en"
      style={{
        ...T.body,
        margin: "0 0 9px",
        textAlign: "justify",
        hyphens: "auto",
        WebkitHyphens: "auto",
        ...style,
      }}
    >
      {children}
    </p>
  );
}

export function Lede({ children, style }: { children: ReactNode; style?: CSSProperties }) {
  return <p style={{ ...T.lede, margin: "0 0 14px", ...style }}>{children}</p>;
}

/** Inline mono: identifiers, constants, schedule names. */
export function M({ children, color }: { children: ReactNode; color?: string }) {
  return (
    <code
      style={{
        fontFamily: F.MONO,
        fontSize: "0.92em",
        color: color ?? C.INK,
        whiteSpace: "nowrap",
      }}
    >
      {children}
    </code>
  );
}

export function Strong({ children }: { children: ReactNode }) {
  return <strong style={{ fontWeight: 600, color: C.INK }}>{children}</strong>;
}

/* ---------- rail notes ---------- */

export function Note({ label, children }: { label?: string; children: ReactNode }) {
  return (
    <div style={{ marginBottom: 14, paddingLeft: 10, borderLeft: `1.5px solid ${C.AMBER_RING}` }}>
      {label && (
        <div style={{ ...T.folio, color: C.AMBER, marginBottom: 3 }}>{label}</div>
      )}
      <div style={{ ...T.monoSmall, color: C.INK_SUBTLE }}>{children}</div>
    </div>
  );
}

/* ---------- figures ---------- */

export function Figure({
  n,
  caption,
  children,
  style,
  pad = true,
}: {
  n: string;
  caption: ReactNode;
  children: ReactNode;
  style?: CSSProperties;
  pad?: boolean;
}) {
  return (
    <figure style={{ margin: "14px 0", ...style }}>
      <div
        style={{
          background: C.PANEL,
          border: `0.5px solid ${C.HAIRLINE}`,
          borderRadius: 3,
          padding: pad ? 14 : 0,
          overflow: "hidden",
        }}
      >
        {children}
      </div>
      <figcaption style={{ ...T.caption, marginTop: 7, display: "flex", gap: 8 }}>
        <span style={{ color: C.AMBER, fontWeight: 500, flexShrink: 0 }}>FIG {n}</span>
        <span>{caption}</span>
      </figcaption>
    </figure>
  );
}

/* ---------- code ---------- */

export function Code({
  title,
  children,
  fontSize = 8.4,
}: {
  title?: string;
  children: string;
  fontSize?: number;
}) {
  return (
    <div
      style={{
        background: C.WELL,
        border: `0.5px solid ${C.HAIRLINE}`,
        borderRadius: 3,
        margin: "12px 0",
        overflow: "hidden",
      }}
    >
      {title && (
        <div
          style={{
            ...T.folio,
            color: C.INK_SUBTLE,
            padding: "6px 12px",
            borderBottom: `0.5px solid ${C.HAIRLINE}`,
            background: C.PANEL,
          }}
        >
          {title}
        </div>
      )}
      <pre
        style={{
          fontFamily: F.MONO,
          fontSize,
          lineHeight: 1.6,
          color: C.INK_MUTED,
          margin: 0,
          padding: "10px 12px",
          whiteSpace: "pre",
          overflow: "hidden",
        }}
      >
        {children}
      </pre>
    </div>
  );
}

/* ---------- tables ---------- */

export function Tbl({
  cols,
  rows,
  align,
  fontSize = 8.2,
  header = true,
}: {
  cols: ReactNode[];
  rows: ReactNode[][];
  /** per-column alignment, "l" | "r" | "c" */
  align?: string;
  fontSize?: number;
  header?: boolean;
}) {
  const alignOf = (i: number): CSSProperties["textAlign"] => {
    const a = align?.[i] ?? "l";
    return a === "r" ? "right" : a === "c" ? "center" : "left";
  };
  return (
    <table
      style={{
        width: "100%",
        borderCollapse: "collapse",
        fontFamily: F.MONO,
        fontSize,
        lineHeight: 1.5,
        margin: "10px 0",
      }}
    >
      {header && (
        <thead>
          <tr>
            {cols.map((c, i) => (
              <th
                key={i}
                style={{
                  ...T.folio,
                  fontSize: 7.2,
                  color: C.INK_SUBTLE,
                  textAlign: alignOf(i),
                  padding: "0 8px 6px 0",
                  borderBottom: `0.5px solid ${C.HAIRLINE_STRONG}`,
                  fontWeight: 500,
                }}
              >
                {c}
              </th>
            ))}
          </tr>
        </thead>
      )}
      <tbody>
        {rows.map((row, r) => (
          <tr key={r}>
            {row.map((cell, i) => (
              <td
                key={i}
                style={{
                  color: C.INK_MUTED,
                  textAlign: alignOf(i),
                  padding: "4.5px 8px 4.5px 0",
                  borderBottom: r < rows.length - 1 ? `0.5px solid ${C.HAIRLINE}` : "none",
                  verticalAlign: "top",
                }}
              >
                {cell}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
