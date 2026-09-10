import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { MetaOutput } from "@rocky-types/meta";
import { ApiError } from "./api";
import { App, EnginePanel } from "./App";
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
});

describe("App", () => {
  it("renders the three lanes and the engine slot", () => {
    render(<App token="t" engine={<span>engine slot</span>} estate={<span>estate slot</span>} />);
    for (const lane of ["Estate", "Review", "Governor"]) {
      expect(screen.getByRole("link", { name: lane })).toHaveAttribute(
        "href",
        `/ui/${lane.toLowerCase()}`,
      );
    }
    expect(screen.getByText("engine slot")).toBeInTheDocument();
  });

  it("switches lanes on a nav click without a reload, and deep-links by path", async () => {
    window.history.pushState(null, "", "/ui/governor");
    render(
      <App
        token="t"
        engine={<span>engine slot</span>}
        estate={<span>estate slot</span>}
        review={<span>review slot</span>}
        governor={<span>governor slot</span>}
      />,
    );
    expect(screen.getByText("governor slot")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Governor" })).toHaveAttribute("aria-current", "page");

    screen.getByRole("link", { name: "Estate" }).click();
    await waitFor(() => expect(screen.getByText("estate slot")).toBeInTheDocument());
    expect(window.location.pathname).toBe("/ui/estate");

    screen.getByRole("link", { name: "Review" }).click();
    await waitFor(() => expect(screen.getByText("review slot")).toBeInTheDocument());
    expect(window.location.pathname).toBe("/ui/review");
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
