import { describe, expect, it } from "vitest";
import { AREAS, areaFromPath } from "./areas";
import { auditPath, custodyPath } from "./router";
import { reviewPath } from "./review/paths";

describe("AREAS", () => {
  it("names the design brief's eleven areas, in its order", () => {
    expect(AREAS.map((area) => area.label)).toEqual([
      "Needs you",
      "Projects",
      "Estate",
      "Runs",
      "Scheduler",
      "Review",
      "Policies",
      "Products",
      "Governance",
      "Agents & Clusters",
      "Settings",
    ]);
  });

  it("links only to routes that existed before the sidebar", () => {
    const links = AREAS.flatMap((area) => (area.kind === "link" ? [[area.id, area.href]] : []));
    expect(Object.fromEntries(links)).toEqual({
      "needs-you": "/ui/governor/brief",
      estate: "/ui/estate",
      review: "/ui/review",
      products: "/ui/governor/products",
      governance: "/ui/governor/scorecard",
    });
  });

  it("gives every disabled area a reason", () => {
    for (const area of AREAS) {
      if (area.kind === "disabled") expect(area.reason.length).toBeGreaterThan(10);
    }
  });

  it("marks each area's own link as that area", () => {
    for (const area of AREAS) {
      if (area.kind === "link") expect(areaFromPath(area.href)).toBe(area.id);
    }
  });
});

describe("areaFromPath", () => {
  it.each([
    // The lane router's own fallbacks survive: the root and anything unknown are Estate.
    ["/ui/", "estate"],
    ["/ui", "estate"],
    ["/ui/nope", "estate"],
    ["/ui/estate", "estate"],
    ["/ui/review", "review"],
    // A bare governor path opens the brief, so it is Needs you, not Governance.
    ["/ui/governor", "needs-you"],
    ["/ui/governor/brief", "needs-you"],
    // An unknown governor tab also opens the brief.
    ["/ui/governor/nope", "needs-you"],
    ["/ui/governor/scorecard", "governance"],
    ["/ui/governor/custody", "governance"],
    ["/ui/governor/audit", "governance"],
    ["/ui/governor/products", "products"],
  ])("puts %s under %s", (path, area) => {
    expect(areaFromPath(path)).toBe(area);
  });

  it("follows every deep link the link helpers build, encoded values included", () => {
    expect(areaFromPath(custodyPath("freeze:global/a b"))).toBe("governance");
    expect(areaFromPath(auditPath("revenue daily"))).toBe("governance");
    expect(areaFromPath(reviewPath("plan/with slash"))).toBe("review");
    expect(areaFromPath(`/ui/governor/products/${encodeURIComponent("a/b")}`)).toBe("products");
  });
});
