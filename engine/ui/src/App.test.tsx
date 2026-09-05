import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { MetaOutput } from "@rocky-types/meta";
import { ApiError } from "./api";
import { App, EnginePanel } from "./App";

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
    await waitFor(() => expect(screen.getByText("1.74.0")).toBeInTheDocument());
    expect(screen.getByText("v23")).toBeInTheDocument();
    expect(screen.getByText("estate, products")).toBeInTheDocument();
  });

  it("renders a hostile engine version as text, never as markup", async () => {
    const hostile = { ...META, engine_version: '<img src=x onerror="alert(1)">' };
    const { container } = render(<EnginePanel token="t" fetchMeta={async () => hostile} />);
    await waitFor(() => expect(screen.getByText(hostile.engine_version)).toBeInTheDocument());
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

  it("explains the missing token instead of calling the engine", () => {
    render(<EnginePanel token={null} />);
    expect(screen.getByText("No token for this tab")).toBeInTheDocument();
  });
});

describe("App", () => {
  it("renders the three lanes and the engine slot", () => {
    render(<App engine={<span>engine slot</span>} estate={<span>estate slot</span>} />);
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
        engine={<span>engine slot</span>}
        estate={<span>estate slot</span>}
        governor={<span>governor slot</span>}
      />,
    );
    expect(screen.getByText("governor slot")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Governor" })).toHaveAttribute("aria-current", "page");

    screen.getByRole("link", { name: "Estate" }).click();
    await waitFor(() => expect(screen.getByText("estate slot")).toBeInTheDocument());
    expect(window.location.pathname).toBe("/ui/estate");

    screen.getByRole("link", { name: "Review" }).click();
    await waitFor(() =>
      expect(screen.getByText("The review screen is not built yet")).toBeInTheDocument(),
    );
    expect(window.location.pathname).toBe("/ui/review");
  });
});
