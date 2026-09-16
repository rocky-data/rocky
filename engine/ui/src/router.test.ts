import { describe, expect, it } from "vitest";
import {
  auditPath,
  laneFromPath,
  navigate,
  pathForLane,
  segmentsFromPath,
  subpathFromPath,
} from "./router";

describe("auditPath", () => {
  it("is the bare ledger without a product", () => {
    expect(auditPath()).toBe("/ui/governor/audit");
    expect(auditPath(null)).toBe("/ui/governor/audit");
  });

  it("carries the product as an encoded segment the governor lane can read back", () => {
    expect(auditPath("revenue_daily")).toBe("/ui/governor/audit/revenue_daily");
    // A name needing escaping round-trips: the segment the lane decodes is the
    // name, and the slash never becomes a path separator (#2003).
    const awkward = "a/b c";
    const path = auditPath(awkward);
    expect(path).toBe(`/ui/governor/audit/${encodeURIComponent(awkward)}`);
    expect(decodeURIComponent(segmentsFromPath(path)[2])).toBe(awkward);
  });
});

describe("subpathFromPath", () => {
  it("reads the segment after the lane, or null", () => {
    expect(subpathFromPath("/ui/governor/scorecard")).toBe("scorecard");
    expect(subpathFromPath("/ui/governor")).toBeNull();
    expect(subpathFromPath("/ui/")).toBeNull();
    expect(pathForLane("governor", "brief")).toBe("/ui/governor/brief");
  });
});

describe("laneFromPath", () => {
  it("selects the estate for the shell's root and for anything unknown", () => {
    for (const path of ["/ui/", "/ui", "/ui/estate", "/ui/estate/", "/ui/nope", "/"]) {
      expect(laneFromPath(path)).toBe("estate");
    }
  });

  it("selects the review and governor lanes by their first segment", () => {
    expect(laneFromPath("/ui/review")).toBe("review");
    expect(laneFromPath("/ui/review/abc")).toBe("review");
    expect(laneFromPath("/ui/governor")).toBe("governor");
  });

  it("round-trips through pathForLane", () => {
    for (const lane of ["estate", "review", "governor"] as const) {
      expect(laneFromPath(pathForLane(lane))).toBe(lane);
    }
  });
});

describe("navigate", () => {
  it("pushes the lane's path and announces it without a reload", () => {
    const pushed: string[] = [];
    const events: string[] = [];
    const win = {
      history: {
        pushState: (_data: unknown, _unused: string, url?: string) => {
          pushed.push(url ?? "");
        },
      },
      dispatchEvent: (event: Event) => {
        events.push(event.type);
        return true;
      },
    } as unknown as Window;
    navigate("review", win);
    expect(pushed).toEqual(["/ui/review"]);
    expect(events).toEqual(["popstate"]);
  });
});
