import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { GovernorScreen } from "./GovernorScreen";

function slots() {
  return {
    brief: <span>brief slot</span>,
    scorecard: <span>scorecard slot</span>,
    custody: (subject: string | null) => <span>custody slot: {subject ?? "none"}</span>,
    audit: <span>audit slot</span>,
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
    await waitFor(() => expect(screen.getByText("audit slot")).toBeInTheDocument());

    screen.getByRole("link", { name: "Custody" }).click();
    await waitFor(() => expect(screen.getByText("custody slot: none")).toBeInTheDocument());
  });

  it("deep-links a custody subject, percent-decoded", () => {
    window.history.pushState(null, "", "/ui/governor/custody/freeze%3Aglobal");
    render(<GovernorScreen {...slots()} />);
    expect(screen.getByText("custody slot: freeze:global")).toBeInTheDocument();
  });
});
