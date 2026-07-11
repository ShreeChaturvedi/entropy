import type { CSSProperties, ReactNode } from "react";
import { C, G, T } from "../theme";

export interface PageProps {
  children: ReactNode;
  /** Running section label shown in the footer, e.g. "01 · Deterministic simulation" */
  section?: string;
  /** Page number injected by App; 0 suppresses the folio. */
  folio?: number;
  /** Full-bleed pages (cover, dividers) draw their own chrome. */
  chrome?: boolean;
  style?: CSSProperties;
}

export function Page({ children, section, folio = 0, chrome = true, style }: PageProps) {
  return (
    <section className="page" style={style}>
      {chrome ? (
        <div
          style={{
            position: "absolute",
            inset: `${G.MARGIN_TOP}px ${G.MARGIN_X}px ${G.MARGIN_BOTTOM}px ${G.MARGIN_X}px`,
            display: "flex",
            flexDirection: "column",
          }}
        >
          {children}
        </div>
      ) : (
        children
      )}
      {chrome && (
        <footer
          style={{
            position: "absolute",
            left: G.MARGIN_X,
            right: G.MARGIN_X,
            bottom: G.FOOTER_Y,
            display: "flex",
            justifyContent: "space-between",
            alignItems: "baseline",
            borderTop: `0.5px solid ${C.HAIRLINE}`,
            paddingTop: 9,
          }}
        >
          <span style={T.folio}>Entropy · Technical Report</span>
          <span style={{ ...T.folio, display: "flex", gap: 14, alignItems: "baseline" }}>
            {section && <span style={{ color: C.INK_SUBTLE }}>{section}</span>}
            {folio > 0 && <span style={{ color: C.AMBER }}>{String(folio).padStart(2, "0")}</span>}
          </span>
        </footer>
      )}
    </section>
  );
}
