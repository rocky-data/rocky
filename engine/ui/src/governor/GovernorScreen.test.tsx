import { render, screen, waitFor } from "@testing-library/react";
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
  it("shows the brief by default and switches tabs without a reload", async () => {
    window.history.pushState(null, "", "/ui/governor");
    render(<GovernorScreen {...slots()} />);
    expect(screen.getByText("brief slot")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Brief" })).toHaveAttribute("aria-current", "page");

    screen.getByRole("link", { name: "Scorecard" }).click();
    await waitFor(() => expect(screen.getByText("scorecard slot")).toBeInTheDocument());
    expect(window.location.pathname).toBe("/ui/governor/scorecard");

    screen.getByRole("link", { name: "Audit" }).click();
    await waitFor(() => expect(screen.getByText("audit slot: none")).toBeInTheDocument());

    screen.getByRole("link", { name: "Custody" }).click();
    await waitFor(() => expect(screen.getByText("custody slot: none")).toBeInTheDocument());

    screen.getByRole("link", { name: "Products" }).click();
    await waitFor(() => expect(screen.getByText("products slot: none")).toBeInTheDocument());
    expect(window.location.pathname).toBe("/ui/governor/products");
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
