import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Clip } from "./components";

describe("Clip", () => {
  /// The marker is the screen's, not the value's. A cut renders it as its
  /// own element; a value that merely contains "…" renders that as text, so
  /// the two can be told apart — before this, `shortId("abcdefghij…")` and a
  /// cut id looked the same (#1815).
  it("renders the cut marker as an element, and a literal ellipsis as text", () => {
    const cut = render(<Clip value={"a".repeat(64)} />);
    const marker = cut.container.querySelector("[data-clipped]");
    expect(marker).not.toBeNull();
    expect(cut.container.textContent).toBe(`${"a".repeat(12)}…`);
    expect(cut.container.querySelector(`[title="${"a".repeat(64)}"]`)).not.toBeNull();
    cut.unmount();

    const literal = render(<Clip value="abcdefghij…" />);
    expect(literal.container.querySelector("[data-clipped]")).toBeNull();
    expect(literal.container.textContent).toBe("abcdefghij…");
  });

  it("keeps both ends when asked, with the marker between them", () => {
    const { container } = render(
      <Clip value="product:revenue_daily@sha256:5b1bf5c@21" keepEnds />,
    );
    expect(container.textContent).toBe("product:re…1bf5c@21");
    expect(container.querySelector("[data-clipped]")?.textContent).toBe("…");
  });
});
