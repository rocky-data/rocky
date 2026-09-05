import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { GovernorScreen } from "./GovernorScreen";

describe("GovernorScreen", () => {
  it("shows the brief by default, deep-links the scorecard, and switches tabs without a reload", async () => {
    window.history.pushState(null, "", "/ui/governor");
    render(<GovernorScreen brief={<span>brief slot</span>} scorecard={<span>scorecard slot</span>} />);
    expect(screen.getByText("brief slot")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Brief" })).toHaveAttribute("aria-current", "page");

    screen.getByRole("link", { name: "Scorecard" }).click();
    await waitFor(() => expect(screen.getByText("scorecard slot")).toBeInTheDocument());
    expect(window.location.pathname).toBe("/ui/governor/scorecard");

    screen.getByRole("link", { name: "Brief" }).click();
    await waitFor(() => expect(screen.getByText("brief slot")).toBeInTheDocument());
    expect(window.location.pathname).toBe("/ui/governor/brief");
  });

  it("selects the scorecard from a deep link", () => {
    window.history.pushState(null, "", "/ui/governor/scorecard");
    render(<GovernorScreen brief={<span>brief slot</span>} scorecard={<span>scorecard slot</span>} />);
    expect(screen.getByText("scorecard slot")).toBeInTheDocument();
  });
});
