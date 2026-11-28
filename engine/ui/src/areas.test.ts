import { describe, expect, it } from "vitest";
import { AREAS, AREA_GROUPS, areaFromPath } from "./areas";
import { auditPath, custodyPath } from "./router";
import { reviewPath } from "./review/paths";

describe("AREAS", () => {
  it("names the design brief's eleven areas, in its order", () => {
    expect(AREAS.map((area) => area.label)).toEqual([
      "Inbox",
      "Projects",
      "Project",
      "Runs",
      "Scheduler",
      "Changes",
      "Policies",
      "Data products",
      "Oversight",
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

  it("groups only areas that open a screen, each once", () => {
    const grouped = AREA_GROUPS.flatMap((group) => group.areas);
    expect(AREA_GROUPS.map((group) => group.label)).toEqual(["Work", "Data", "Govern"]);
    expect(new Set(grouped).size).toBe(grouped.length);
    for (const id of grouped) expect(AREAS.find((area) => area.id === id)?.kind).toBe("link");
    // Every link is in a group, so none is lost.
    const links = AREAS.filter((area) => area.kind === "link").map((area) => area.id);
    expect([...grouped].sort()).toEqual([...links].sort());
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
    // The lane router's own fallbacks survive: the root and anything unknown are Project.
    ["/ui/", "estate"],
    ["/ui", "estate"],
    ["/ui/nope", "estate"],
    ["/ui/estate", "estate"],
    ["/ui/review", "review"],
    // A bare governor path opens the brief, so it is Inbox, not Oversight.
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
