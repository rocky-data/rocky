import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { MetaOutput } from "@rocky-types/meta";
import { ApiError } from "./api";
import { App, EnginePanel, WIDE_ENOUGH_FOR_THE_SIDEBAR } from "./App";
import { GovernorScreen } from "./governor/GovernorScreen";
import { TOKEN_STORAGE_KEY } from "./token";

const META: MetaOutput = {
  version: "1.74.0",
  command: "meta",
  engine_version: "1.74.0",
  state_schema_version: 23,
  schemas_hash: "abc",
  routes: ["GET /api/v1/meta"],
  capabilities: ["estate", "products"],
};

describe("EnginePanel", () => {
  it("renders the engine version and the capabilities", async () => {
    render(<EnginePanel token="t" fetchMeta={async () => META} />);
    await waitFor(() => expect(screen.getByText("rocky 1.74.0")).toBeInTheDocument());
    expect(screen.getByText(/state schema v23/)).toBeInTheDocument();
    expect(screen.getByText("2 capabilities")).toHaveAttribute("title", "estate, products");
  });

  it("renders a hostile engine version as text, never as markup", async () => {
    const hostile = { ...META, engine_version: '<img src=x onerror="alert(1)">' };
    const { container } = render(<EnginePanel token="t" fetchMeta={async () => hostile} />);
    await waitFor(() =>
      expect(screen.getByText(`rocky ${hostile.engine_version}`)).toBeInTheDocument(),
    );
    expect(container.querySelector("img")).toBeNull();
  });

  it("shows the envelope when the engine refuses", async () => {
    const refused = new ApiError(401, {
      code: "unauthorized",
      message: "missing bearer",
      remediation_hint: "open the printed address",
    });
    render(
      <EnginePanel
        token="t"
        fetchMeta={async () => {
          throw refused;
        }}
      />,
    );
    await waitFor(() => expect(screen.getByText("unauthorized")).toBeInTheDocument());
    expect(screen.getByText("open the printed address")).toBeInTheDocument();
  });

  it("is one line, and that line carries all three facts", async () => {
    // Row 3 asked for a screenshot. This environment has no browser (the
    // Chrome extension is not connected and no headless browser is
    // installed), so the claim is pinned mechanically instead: the whole
    // engine block is a single element whose text is one line.
    const { container } = render(<EnginePanel token="t" fetchMeta={async () => META} />);
    await waitFor(() => expect(screen.getByText("rocky 1.74.0")).toBeInTheDocument());

    const line = container.firstElementChild as HTMLElement;
    expect(line.tagName).toBe("P");
    // What a sighted reader sees, with the screen-reader-only text removed.
    // The separators are spaced with CSS padding, not literal spaces, so the
    // text node itself has none around the dots.
    const visible = line.cloneNode(true) as HTMLElement;
    visible.querySelectorAll(".sr-only, [hidden]").forEach((node) => node.remove());
    expect(visible.textContent).toBe("rocky 1.74.0·state schema v23·2 capabilities");
    // The names reach a mouse via `title` and assistive technology via a
    // hidden description. Hidden, so it is not read a second time in flow.
    const count = screen.getByText("2 capabilities");
    expect(count).toHaveAttribute("title", "estate, products");
    const describedBy = count.getAttribute("aria-describedby");
    const description = describedBy ? document.getElementById(describedBy) : null;
    expect(description?.textContent).toBe("estate, products");
    expect(description).toHaveAttribute("hidden");
    // Three cards became one line, so no grid survives.
    expect(container.querySelector(".grid")).toBeNull();
  });

  it("explains the missing token instead of calling the engine", () => {
    render(<EnginePanel token={null} />);
    expect(screen.getByText("No token for this tab")).toBeInTheDocument();
  });

  describe("how often it asks", () => {
    // These render the PRODUCTION seam: `<EnginePanel />` with no loader, the
    // way `App` renders it. A test that passes its own loader pins a stable
    // function and cannot see the defect — an idle tab asked the engine about
    // 15 times a second, for as long as it was open (#2075).
    afterEach(() => {
      vi.unstubAllGlobals();
      window.sessionStorage.clear();
    });

    function countingFetch() {
      const calls: string[] = [];
      vi.stubGlobal(
        "fetch",
        vi.fn(async (input: RequestInfo | URL) => {
          calls.push(String(input));
          return new Response(JSON.stringify(META), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }),
      );
      return calls;
    }

    it("asks once, and does not ask again while the tab sits there", async () => {
      window.sessionStorage.setItem(TOKEN_STORAGE_KEY, "t");
      const calls = countingFetch();
      render(<EnginePanel />);

      await waitFor(() => expect(screen.getByText("rocky 1.74.0")).toBeInTheDocument());
      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(calls.filter((url) => url.includes("/api/v1/meta"))).toHaveLength(1);
    });

    it("does not ask again when the page around it renders", async () => {
      window.sessionStorage.setItem(TOKEN_STORAGE_KEY, "t");
      const calls = countingFetch();
      function Around() {
        const [tick, setTick] = useState(0);
        return (
          <>
            <button type="button" onClick={() => setTick((t) => t + 1)}>
              render again ({tick})
            </button>
            <EnginePanel />
          </>
        );
      }
      render(<Around />);
      await waitFor(() => expect(screen.getByText("rocky 1.74.0")).toBeInTheDocument());

      for (let i = 0; i < 3; i++) fireEvent.click(screen.getByRole("button", { name: /render again/ }));
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(calls.filter((url) => url.includes("/api/v1/meta"))).toHaveLength(1);
    });
  });
});

describe("App", () => {
  const slots = {
    engine: <span>engine slot</span>,
    estate: <span>estate slot</span>,
    review: <span>review slot</span>,
    governor: <span>governor slot</span>,
  };

  /**
   * The areas nav a screen reader would read, so a Governor tab of the same
   * name never matches — and neither does the fixed sidebar while the drawer
   * is open, since the dialog puts `aria-hidden` on the rest of the page.
   */
  function liveAreasNav(): HTMLElement {
    const navs = [...document.querySelectorAll('nav[aria-label="Areas"]')].filter(
      (nav) => nav.closest('[aria-hidden="true"]') === null,
    );
    expect(navs).toHaveLength(1);
    return navs[0] as HTMLElement;
  }
  const areas = () => within(liveAreasNav());

  it("shows the Rocky mark beside the name, and says nothing twice", () => {
    window.history.pushState(null, "", "/ui/estate");
    const { container } = render(<App token="t" {...slots} />);
    // Two wordmarks with the drawer closed: the sidebar's and the narrow
    // bar's. CSS shows one at a time; both carry the mark.
    const marks = container.querySelectorAll("img");
    expect(marks).toHaveLength(2);
    for (const mark of marks) {
      // Decorative: the name beside it is the text a screen reader reads.
      expect(mark).toHaveAttribute("alt", "");
      // The build inlines a mark this small as a data URI and emits larger
      // ones under `assets/`. Either is the page itself; nothing may be
      // remote, which the page's CSP would refuse anyway. Resolved against
      // the page, so `//other.example/x.svg` counts as remote too.
      const src = mark.getAttribute("src") ?? "";
      if (src.startsWith("data:")) {
        expect(src).toMatch(/^data:image\/svg\+xml/);
      } else {
        expect(new URL(src, window.location.href).origin).toBe(window.location.origin);
        expect(src).toMatch(/rocky-logo.*\.svg$/);
      }
      expect(mark.closest("span")?.textContent).toBe("Rocky");
    }
  });

  it("renders the eleven areas: five links, six disabled with their reasons", () => {
    window.history.pushState(null, "", "/ui/estate");
    render(<App token="t" {...slots} />);
    const nav = liveAreasNav();
    // Five that open a screen, then the six that do not, under their heading.
    expect(nav.textContent).toContain("No page yet");
    expect(within(nav).getAllByRole("link").map((link) => link.textContent)).toEqual([
      "Needs you",
      "Estate",
      "Review",
      "Products",
      "Governance",
    ]);
    const disabled = nav.querySelectorAll('[aria-disabled="true"]');
    expect(disabled).toHaveLength(6);
    // Not a link, so not in the tab order; its reason is on the page.
    for (const entry of disabled) expect(entry.closest("a")).toBeNull();
    expect(within(nav).getByText("No page of its own yet. The runs table is on Estate.")).toBeInTheDocument();
    expect(screen.getByText("engine slot")).toBeInTheDocument();
  });

  it("switches areas on a click without a reload, and marks exactly one current", async () => {
    window.history.pushState(null, "", "/ui/governor");
    render(<App token="t" {...slots} />);
    expect(screen.getByText("governor slot")).toBeInTheDocument();
    // A bare governor path opens the brief, so Needs you is current.
    expect(areas().getByRole("link", { name: "Needs you" })).toHaveAttribute("aria-current", "page");

    areas().getByRole("link", { name: "Estate" }).click();
    await waitFor(() => expect(screen.getByText("estate slot")).toBeInTheDocument());
    expect(window.location.pathname).toBe("/ui/estate");

    areas().getByRole("link", { name: "Governance" }).click();
    await waitFor(() => expect(window.location.pathname).toBe("/ui/governor/scorecard"));
    expect(screen.getByText("governor slot")).toBeInTheDocument();

    const current = screen
      .getByRole("navigation", { name: "Areas" })
      .querySelectorAll("[aria-current]");
    expect([...current].map((node) => node.textContent)).toEqual(["Governance"]);
  });

  it.each([
    // Governance has tabs, so its sidebar entry is the current section ("true").
    ["/ui/governor/custody/freeze%3Aglobal", "Governance", "true", "governor slot"],
    ["/ui/governor/audit/revenue%20daily", "Governance", "true", "governor slot"],
    ["/ui/governor/products/revenue%20daily", "Products", "page", "governor slot"],
    ["/ui/review/plan-1", "Review", "page", "review slot"],
    ["/ui/nope", "Estate", "page", "estate slot"],
  ])("deep-links %s under %s", (path, area, mark, slot) => {
    window.history.pushState(null, "", path);
    render(<App token="t" {...slots} />);
    expect(screen.getByText(slot)).toBeInTheDocument();
    expect(areas().getByRole("link", { name: area })).toHaveAttribute("aria-current", mark);
  });

  it.each([
    ["/ui/governor", "Needs you"],
    ["/ui/governor/scorecard", "Scorecard"],
    ["/ui/governor/custody/freeze%3Aglobal", "Custody"],
    ["/ui/governor/audit", "Audit"],
    ["/ui/governor/products", "Products"],
    ["/ui/estate", "Estate"],
  ])("marks exactly one current page on the whole page at %s", (path, page) => {
    // The real governor tab bar, not a slot: the defect this pins was a tab
    // and a sidebar entry both claiming the page, which a slot cannot show.
    window.history.pushState(null, "", path);
    const governor = (
      <GovernorScreen
        brief={<span>brief slot</span>}
        scorecard={<span>scorecard slot</span>}
        custody={() => <span>custody slot</span>}
        audit={() => <span>audit slot</span>}
        products={() => <span>products slot</span>}
      />
    );
    render(<App token="t" {...slots} governor={governor} />);
    const pages = document.querySelectorAll('[aria-current="page"]');
    expect([...pages].map((node) => node.textContent)).toEqual([page]);
  });

  it("follows Back and Forward", async () => {
    window.history.pushState(null, "", "/ui/estate");
    render(<App token="t" {...slots} />);
    areas().getByRole("link", { name: "Review" }).click();
    await waitFor(() => expect(screen.getByText("review slot")).toBeInTheDocument());

    act(() => {
      window.history.back();
    });
    await waitFor(() => expect(screen.getByText("estate slot")).toBeInTheDocument());
    expect(areas().getByRole("link", { name: "Estate" })).toHaveAttribute("aria-current", "page");
  });

  describe("the drawer below the breakpoint", () => {
    /**
     * The button that opens the drawer, captured as a node: once the dialog
     * is open it sits behind `aria-hidden`, so a role query cannot find it.
     */
    const openButton = () => screen.getByRole("button", { name: "Areas" });

    it("names the current area in the bar, and opens the areas in a dialog", async () => {
      window.history.pushState(null, "", "/ui/review");
      render(<App token="t" {...slots} />);
      // The bar names where you are, since the sidebar is folded away.
      expect(screen.getByText("Review", { selector: "div" })).toBeInTheDocument();
      const button = openButton();
      expect(button).toHaveAttribute("aria-expanded", "false");
      expect(document.querySelectorAll('[role="dialog"]')).toHaveLength(0);

      fireEvent.click(button);
      const dialog = await screen.findByRole("dialog");
      expect(button).toHaveAttribute("aria-expanded", "true");
      expect(within(dialog).getByRole("link", { name: "Estate" })).toBeInTheDocument();
    });

    it("leaves one set of areas in the reading order while it is open", async () => {
      window.history.pushState(null, "", "/ui/estate");
      render(<App token="t" {...slots} />);
      const button = openButton();
      fireEvent.click(button);
      await screen.findByRole("dialog");

      // Two navs exist in the page — the fixed one and the drawer's — but the
      // dialog hides the rest of the page, so only one is read, and only one
      // link says it is the current page.
      expect(document.querySelectorAll('nav[aria-label="Areas"]')).toHaveLength(2);
      expect(liveAreasNav().closest('[role="dialog"]')).not.toBeNull();
      const announced = [...document.querySelectorAll('[aria-current="page"]')].filter(
        (node) => node.closest('[aria-hidden="true"]') === null,
      );
      expect(announced.map((node) => node.textContent)).toEqual(["Estate"]);
      // And the engine line is drawn twice but read once, from one request.
      expect(screen.getAllByText("engine slot")).toHaveLength(2);
    });

    it("closes on a navigation from inside it", async () => {
      window.history.pushState(null, "", "/ui/estate");
      render(<App token="t" {...slots} />);
      const button = openButton();
      fireEvent.click(button);
      const dialog = await screen.findByRole("dialog");
      within(dialog).getByRole("link", { name: "Review" }).click();
      await waitFor(() => expect(button).toHaveAttribute("aria-expanded", "false"));
      expect(window.location.pathname).toBe("/ui/review");
    });

    it("closes on Back", async () => {
      window.history.pushState(null, "", "/ui/estate");
      window.history.pushState(null, "", "/ui/review");
      render(<App token="t" {...slots} />);
      const button = openButton();
      fireEvent.click(button);
      await screen.findByRole("dialog");
      act(() => {
        window.history.back();
      });
      await waitFor(() => expect(screen.getByText("estate slot")).toBeInTheDocument());
      expect(button).toHaveAttribute("aria-expanded", "false");
    });

    it("closes on Escape and gives focus back to the button", async () => {
      window.history.pushState(null, "", "/ui/estate");
      render(<App token="t" {...slots} />);
      const button = openButton();
      fireEvent.click(button);
      await screen.findByRole("dialog");
      fireEvent.keyDown(document, { key: "Escape" });
      await waitFor(() => expect(button).toHaveAttribute("aria-expanded", "false"));
      // The dialog restores focus itself, after it has closed.
      await waitFor(() => expect(document.activeElement).toBe(button));
    });

    it("closes when the window grows past the breakpoint", async () => {
      // Left open there, the dialog would keep its focus trap and its hold on
      // the page over a sidebar the viewer can now see anyway.
      const listeners: ((event: MediaQueryListEvent) => void)[] = [];
      vi.stubGlobal(
        "matchMedia",
        vi.fn((query: string) => ({
          matches: false,
          media: query,
          addEventListener: (_: string, fn: (event: MediaQueryListEvent) => void) =>
            listeners.push(fn),
          removeEventListener: vi.fn(),
        })),
      );
      window.history.pushState(null, "", "/ui/estate");
      render(<App token="t" {...slots} />);
      expect(window.matchMedia).toHaveBeenCalledWith(WIDE_ENOUGH_FOR_THE_SIDEBAR);
      const button = openButton();
      fireEvent.click(button);
      await screen.findByRole("dialog");

      act(() => {
        for (const fn of listeners) fn({ matches: true } as MediaQueryListEvent);
      });
      await waitFor(() => expect(button).toHaveAttribute("aria-expanded", "false"));
      vi.unstubAllGlobals();
    });

    it("reads the engine once, however often the drawer opens", async () => {
      // The sidebar is drawn twice; two reads of `/api/v1/meta` would be the
      // shape of #2075 again, one level up.
      const calls: string[] = [];
      vi.stubGlobal(
        "fetch",
        vi.fn(async (input: RequestInfo | URL) => {
          calls.push(String(input));
          return new Response(JSON.stringify(META), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }),
      );
      window.history.pushState(null, "", "/ui/estate");
      render(<App token="t" estate={<span>estate slot</span>} />);
      await waitFor(() => expect(calls.some((url) => url.includes("/api/v1/meta"))).toBe(true));

      const button = openButton();
      fireEvent.click(button);
      await screen.findByRole("dialog");
      fireEvent.keyDown(document, { key: "Escape" });
      await waitFor(() => expect(button).toHaveAttribute("aria-expanded", "false"));
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(calls.filter((url) => url.includes("/api/v1/meta"))).toHaveLength(1);
      vi.unstubAllGlobals();
    });
  });
});

describe("the no-token page", () => {
  // The whole point of one boundary is that this is true for every lane at
  // once. So these render the REAL lanes — passing a slot would prove nothing
  // about the screen that actually reads the API.
  function countingFetch() {
    const calls: string[] = [];
    const stub = vi.fn(async (input: RequestInfo | URL) => {
      calls.push(String(input));
      return new Response("{}", { status: 200, headers: { "content-type": "application/json" } });
    });
    vi.stubGlobal("fetch", stub);
    return calls;
  }

  afterEach(() => {
    vi.unstubAllGlobals();
    window.sessionStorage.clear();
  });

  it("keeps the engine line inside the token boundary, though it sits in the sidebar", () => {
    // The sidebar renders at every width and for every path, including with no
    // token. The engine line reads the API, so it must not render there.
    render(<App token={null} engine={<span>engine slot</span>} />);
    expect(screen.getByText("No token for this tab")).toBeInTheDocument();
    expect(screen.queryByText("engine slot")).toBeNull();
    expect(screen.queryByRole("region", { name: "Engine" })).toBeNull();
    // The areas still show: they read nothing.
    expect(screen.getByRole("navigation", { name: "Areas" })).toBeInTheDocument();
  });

  it.each(["estate", "review", "governor"])(
    "issues no request at all on the %s lane",
    async (lane) => {
      const calls = countingFetch();
      window.history.pushState(null, "", `/ui/${lane}`);
      render(<App token={null} />);

      expect(screen.getByText("No token for this tab")).toBeInTheDocument();
      // Give any effect that was going to fire the chance to fire.
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(calls).toEqual([]);
    },
  );

  it.each(["estate", "review", "governor"])(
    "does issue requests on the %s lane once a token is present",
    async (lane) => {
      // Without this the assertion above is vacuous: it would also pass if
      // the lanes never called `fetch` for some unrelated reason.
      const calls = countingFetch();
      window.history.pushState(null, "", `/ui/${lane}`);
      render(<App token="t" />);

      await waitFor(() => expect(calls.length).toBeGreaterThan(0));
      expect(screen.queryByText("No token for this tab")).not.toBeInTheDocument();
    },
  );
});

describe("the production token seam", () => {
  // The tests above pass `token` explicitly, which proves the boundary but
  // not the thing production uses. `main.tsx` renders a bare `<App />`, so
  // the default parameter IS the production path: swap it for any non-null
  // value and every test above stays green while the real page mounts lanes
  // with no credentials. These render `<App />` with nothing passed.
  function countingFetch() {
    const calls: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        calls.push(String(input));
        return new Response("{}", {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }),
    );
    return calls;
  }

  afterEach(() => {
    vi.unstubAllGlobals();
    window.sessionStorage.clear();
  });

  it("reads the token storage actually holds, not one handed in", async () => {
    window.sessionStorage.setItem(TOKEN_STORAGE_KEY, "from-storage");
    const calls = countingFetch();
    window.history.pushState(null, "", "/ui/estate");
    render(<App />);

    await waitFor(() => expect(calls.length).toBeGreaterThan(0));
    expect(screen.queryByText("No token for this tab")).not.toBeInTheDocument();
  });

  it("gates when storage is empty", async () => {
    window.sessionStorage.clear();
    const calls = countingFetch();
    window.history.pushState(null, "", "/ui/estate");
    render(<App />);

    expect(screen.getByText("No token for this tab")).toBeInTheDocument();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(calls).toEqual([]);
  });

  it("treats a stored empty string as no token, not as one", async () => {
    // The two readers must agree. `apiGet` sends no Authorization header for
    // "" because it is falsy, so a shell that accepted "" would mount every
    // lane and fetch without credentials — the wall of 401s, back again.
    window.sessionStorage.setItem(TOKEN_STORAGE_KEY, "");
    const calls = countingFetch();
    window.history.pushState(null, "", "/ui/review");
    render(<App />);

    expect(screen.getByText("No token for this tab")).toBeInTheDocument();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(calls).toEqual([]);
  });
});
