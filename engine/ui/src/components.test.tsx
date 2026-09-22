import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Clip, StatusCard, type Tone } from "./components";

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

describe("StatusCard", () => {
  // `border-zinc-200`/`dark:border-zinc-700` set the CSS `border-color`
  // shorthand, which resets `border-left-color` along with it -- and the
  // built stylesheet compiles `dark:border-zinc-700` after every tone's
  // `border-l-{colour}-500` rule, so every card showed the same grey left
  // border in dark mode, whatever its tone (#2123, confirmed by inspecting
  // `vite build`'s output CSS). jsdom does not resolve the CSS cascade, so
  // this pins the class list itself: the neutral border must be three
  // directional utilities that never touch the left side, leaving the tone
  // class's `border-l-*` the only thing that ever sets it.
  const TONE_ACCENT: Record<Tone, string> = {
    ok: "border-l-emerald-500",
    warn: "border-l-amber-500",
    risk: "border-l-red-500",
    muted: "border-l-zinc-400",
    pending: "border-l-zinc-400",
  };

  it.each(Object.entries(TONE_ACCENT) as [Tone, string][])(
    "keeps the %s tone's own accent, with no shorthand that can reset it",
    (tone, accent) => {
      const { container } = render(<StatusCard label="Status" value="x" tone={tone} />);
      const card = container.firstElementChild as HTMLElement;
      expect(card).toHaveClass(accent);
      expect(card).not.toHaveClass("border-zinc-200");
      expect(card).not.toHaveClass("dark:border-zinc-700");
      expect(card).toHaveClass(
        "border-t-zinc-200",
        "border-r-zinc-200",
        "border-b-zinc-200",
        "dark:border-t-zinc-700",
        "dark:border-r-zinc-700",
        "dark:border-b-zinc-700",
      );
    },
  );
});
