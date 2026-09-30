/**
 * The shell's eleven areas, and which one a path belongs to.
 *
 * An area either opens a screen that exists, at a route that already existed
 * before the sidebar, or is disabled with its reason. No route was renamed for
 * the sidebar: deep links, the link helpers in `router.ts` and the docs'
 * screenshot script all navigate by address.
 */

import type { ComponentType, SVGProps } from "react";
import {
  BellAlertIcon,
  CalendarDaysIcon,
  ClipboardDocumentCheckIcon,
  Cog6ToothIcon,
  CpuChipIcon,
  CubeIcon,
  FolderIcon,
  PlayCircleIcon,
  ScaleIcon,
  ShieldCheckIcon,
  Squares2X2Icon,
} from "@heroicons/react/24/outline";
import { GOVERNOR_TABS, governorAreaOf, governorTabFromSegment } from "./governor/tabs";
import { UI_BASE, laneFromPath, segmentsFromPath } from "./router";

/** An area's icon: decorative, beside the name that carries the meaning. */
export type AreaIcon = ComponentType<SVGProps<SVGSVGElement>>;

export type AreaId =
  | "needs-you"
  | "projects"
  | "estate"
  | "runs"
  | "scheduler"
  | "review"
  | "policies"
  | "products"
  | "governance"
  | "agents"
  | "settings";

export type Area =
  | {
      readonly id: AreaId;
      readonly label: string;
      readonly icon: AreaIcon;
      readonly kind: "link";
      readonly href: string;
    }
  | {
      readonly id: AreaId;
      readonly label: string;
      readonly icon: AreaIcon;
      readonly kind: "disabled";
      readonly reason: string;
    };

/**
 * In the order of the design brief. Each disabled reason says what is true
 * today, and where to look instead when something exists elsewhere: a route
 * with no page is not the same as nothing at all.
 */
export const AREAS: readonly Area[] = [
  {
    id: "needs-you",
    label: "Needs you",
    icon: BellAlertIcon,
    kind: "link",
    href: `${UI_BASE}/governor/brief`,
  },
  {
    id: "projects",
    label: "Projects",
    icon: FolderIcon,
    kind: "disabled",
    reason: "One project for now: the one this server runs.",
  },
  { id: "estate", label: "Estate", icon: Squares2X2Icon, kind: "link", href: `${UI_BASE}/estate` },
  {
    id: "runs",
    label: "Runs",
    icon: PlayCircleIcon,
    kind: "disabled",
    reason: "No page of its own yet. The runs table is on Estate.",
  },
  {
    id: "scheduler",
    label: "Scheduler",
    icon: CalendarDaysIcon,
    kind: "disabled",
    reason: "No page of its own yet. The schedule status is on Estate.",
  },
  {
    id: "review",
    label: "Review",
    icon: ClipboardDocumentCheckIcon,
    kind: "link",
    href: `${UI_BASE}/review`,
  },
  {
    id: "policies",
    label: "Policies",
    icon: ShieldCheckIcon,
    kind: "disabled",
    reason: "No page yet. The engine serves the rules at /api/v1/policy.",
  },
  {
    id: "products",
    label: "Products",
    icon: CubeIcon,
    kind: "link",
    href: `${UI_BASE}/governor/products`,
  },
  {
    id: "governance",
    label: "Governance",
    icon: ScaleIcon,
    kind: "link",
    href: `${UI_BASE}/governor/scorecard`,
  },
  {
    id: "agents",
    label: "Agents & Clusters",
    icon: CpuChipIcon,
    kind: "disabled",
    reason: "No page yet. Agent activity is on Needs you.",
  },
  {
    id: "settings",
    label: "Settings",
    icon: Cog6ToothIcon,
    kind: "disabled",
    reason: "No page yet. The engine serves them at /api/v1/settings.",
  },
];

/** The label above the areas that open nothing yet. */
export const NOT_YET_HEADING = "No page yet";

/**
 * Whether an area's screen carries its own tab bar. Governance does: its
 * Scorecard, Custody and Audit tabs. There the tab is the current page, and
 * the sidebar entry is only the current section — two `aria-current="page"`
 * marks on one page would tell a screen reader two different things are it.
 */
export function areaHasTabs(id: AreaId): boolean {
  return GOVERNOR_TABS.filter((tab) => tab.area === id).length > 1;
}

/**
 * The area a path belongs to. It follows the lane router, so an unknown path
 * is Estate, as it always was; inside the governor lane it follows the lane's
 * own tab rule, so a bare `/ui/governor` is Needs you, because it opens the
 * brief.
 */
export function areaFromPath(pathname: string): AreaId {
  switch (laneFromPath(pathname)) {
    case "estate":
      return "estate";
    case "review":
      return "review";
    case "governor":
      return governorAreaOf(governorTabFromSegment(segmentsFromPath(pathname)[1]));
  }
}
