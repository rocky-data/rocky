import { describe, expect, it } from "vitest";
import { laneFromPath, navigate, pathForLane } from "./router";

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
