import { Children, cloneElement, isValidElement, type ReactElement } from "react";
import { Cover } from "./pages/Cover";
import { Abstract } from "./pages/FrontMatter";
import { PartDivider } from "./components/Divider";
import { ch1Pages } from "./pages/Ch1Simulation";
import { ch2Pages } from "./pages/Ch2Bugs";
import { ch3Pages } from "./pages/Ch3Engine";
import { ch4Pages } from "./pages/Ch4Query";
import { ch5Pages } from "./pages/Ch5Tui";
import { ch6Pages } from "./pages/Ch6Perf";
import { ch7Pages } from "./pages/Ch7Limits";
import { Reproduction, BackCover } from "./pages/BackMatter";

const pages = [
  <Cover key="cover" />,
  <Abstract key="abstract" />,
  <PartDivider
    key="div1"
    numeral="I"
    title={<>Faults, by design</>}
    seed={0x11}
    items={[
      { n: "01", label: "Crash recovery as a falsifiable claim" },
      { n: "02", label: "Two bugs the test suite never caught" },
    ]}
  />,
  ...ch1Pages,
  ...ch2Pages,
  <PartDivider
    key="div2"
    numeral="II"
    title={<>The machine</>}
    seed={0x22}
    items={[
      { n: "03", label: "The engine under test" },
      { n: "04", label: "From SQL text to a costed plan" },
    ]}
  />,
  ...ch3Pages,
  ...ch4Pages,
  <PartDivider
    key="div3"
    numeral="III"
    title={<>Instruments and evidence</>}
    seed={0x33}
    items={[
      { n: "05", label: "An instrument panel for crashes" },
      { n: "06", label: "Measured, on two machines" },
      { n: "07", label: "What this engine does not do" },
    ]}
  />,
  ...ch5Pages,
  ...ch6Pages,
  ...ch7Pages,
  <Reproduction key="repro" />,
  <BackCover key="back" />,
];

export default function App() {
  return (
    <div className="report-root">
      {Children.map(pages, (el, i) =>
        isValidElement(el) ? cloneElement(el as ReactElement<{ folio?: number }>, { folio: i + 1 }) : el,
      )}
    </div>
  );
}
