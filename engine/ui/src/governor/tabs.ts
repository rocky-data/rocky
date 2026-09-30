/**
 * The governor lane's screens, and which sidebar area each belongs to.
 *
 * One module, so the shell's sidebar and the lane's own tab bar decide from
 * the same rule. Before the sidebar, the lane showed all five as tabs; now the
 * brief is "Needs you" and the product timelines are "Products", each an area
 * of its own, and the remaining three are "Governance".
 */

export const GOVERNOR_TABS = [
  { id: "brief", label: "Brief", producer: "GET /api/v1/brief", area: "needs-you" },
  { id: "scorecard", label: "Scorecard", producer: "GET /api/v1/audit/scorecard", area: "governance" },
  { id: "custody", label: "Custody", producer: "GET /api/v1/custody/{subject}", area: "governance" },
  { id: "audit", label: "Audit", producer: "GET /api/v1/audit", area: "governance" },
  {
    id: "products",
    label: "Products",
    producer: "GET /api/v1/products/{name}/journal",
    area: "products",
  },
] as const;

export type GovernorTab = (typeof GOVERNOR_TABS)[number]["id"];
export type GovernorArea = (typeof GOVERNOR_TABS)[number]["area"];

/**
 * The tab a path segment selects. No segment, or one this lane does not name,
 * is the brief: `/ui/governor` has always opened it.
 */
export function governorTabFromSegment(segment: string | undefined): GovernorTab {
  switch (segment) {
    case "scorecard":
    case "custody":
    case "audit":
    case "products":
      return segment;
    default:
      return "brief";
  }
}

/** The area a governor tab belongs to. */
export function governorAreaOf(tab: GovernorTab): GovernorArea {
  return GOVERNOR_TABS.find((entry) => entry.id === tab)?.area ?? "needs-you";
}
