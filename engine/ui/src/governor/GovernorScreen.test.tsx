import { render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { GovernorScreen } from "./GovernorScreen";

function slots() {
  return {
    brief: <span>brief slot</span>,
    scorecard: <span>scorecard slot</span>,
    custody: (subject: string | null) => <span>custody slot: {subject ?? "none"}</span>,
    audit: (product: string | null) => <span>audit slot: {product ?? "none"}</span>,
    products: (name: string | null) => <span>products slot: {name ?? "none"}</span>,
  };
}

describe("GovernorScreen", () => {
  it("shows the brief by default, with no tab bar: it is an area of its own", () => {
    window.history.pushState(null, "", "/ui/governor");
    render(<GovernorScreen {...slots()} />);
    expect(screen.getByText("brief slot")).toBeInTheDocument();
    // The sidebar's "Needs you" is the one navigation that marks this page.
    expect(screen.queryByRole("navigation", { name: "Governor screens" })).toBeNull();
  });

  it("shows no tab bar on Products, an area of its own", () => {
    window.history.pushState(null, "", "/ui/governor/products");
    render(<GovernorScreen {...slots()} />);
    expect(screen.getByText("products slot: none")).toBeInTheDocument();
    expect(screen.queryByRole("navigation", { name: "Governor screens" })).toBeNull();
  });

  it("switches between Governance's three tabs without a reload", async () => {
    window.history.pushState(null, "", "/ui/governor/scorecard");
    render(<GovernorScreen {...slots()} />);
    const tabs = within(screen.getByRole("navigation", { name: "Governor screens" }));
    expect(tabs.getAllByRole("link").map((link) => link.textContent)).toEqual([
      "Scorecard",
      "Custody",
      "Audit",
    ]);
    expect(tabs.getByRole("link", { name: "Scorecard" })).toHaveAttribute("aria-current", "page");

    tabs.getByRole("link", { name: "Audit" }).click();
    await waitFor(() => expect(screen.getByText("audit slot: none")).toBeInTheDocument());

    screen.getByRole("link", { name: "Custody" }).click();
    await waitFor(() => expect(screen.getByText("custody slot: none")).toBeInTheDocument());
    expect(window.location.pathname).toBe("/ui/governor/custody");
  });

  it("deep-links one product's timeline", () => {
    window.history.pushState(null, "", "/ui/governor/products/revenue%20daily");
    render(<GovernorScreen {...slots()} />);
    expect(screen.getByText("products slot: revenue daily")).toBeInTheDocument();
  });

  it("deep-links a custody subject, percent-decoded", () => {
    window.history.pushState(null, "", "/ui/governor/custody/freeze%3Aglobal");
    render(<GovernorScreen {...slots()} />);
    expect(screen.getByText("custody slot: freeze:global")).toBeInTheDocument();
  });

  /// The Products tab sends a reader here rather than to custody, which has no
  /// answer for a product (#2003), so the scope has to survive the link.
  it("deep-links the ledger scoped to one product, percent-decoded", () => {
    window.history.pushState(null, "", "/ui/governor/audit/revenue%20daily");
    render(<GovernorScreen {...slots()} />);
    expect(screen.getByText("audit slot: revenue daily")).toBeInTheDocument();
  });
});
