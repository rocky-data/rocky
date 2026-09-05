import type { ReactNode } from "react";
import { navigateTo, pathForLane, useSubpath } from "../router";
import { BriefScreen } from "./BriefScreen";
import { ScorecardScreen } from "./ScorecardScreen";

const TABS = [
  { id: "brief", label: "Brief", producer: "GET /api/v1/brief" },
  { id: "scorecard", label: "Scorecard", producer: "GET /api/v1/audit/scorecard" },
] as const;

type Tab = (typeof TABS)[number]["id"];

function tabFromSubpath(subpath: string | null): Tab {
  return subpath === "scorecard" ? "scorecard" : "brief";
}

/**
 * The governor lane: the brief and the trust scorecard, one tab each,
 * deep-linked at `/ui/governor/brief` and `/ui/governor/scorecard`.
 * The custody drill-down and the audit browse are U4-P2.
 */
export function GovernorScreen({ brief, scorecard }: { brief?: ReactNode; scorecard?: ReactNode }) {
  const tab = tabFromSubpath(useSubpath());
  return (
    <div className="space-y-4">
      <nav aria-label="Governor screens" className="flex gap-4 border-b border-zinc-200 text-sm dark:border-zinc-800">
        {TABS.map((entry) => (
          <a
            key={entry.id}
            href={pathForLane("governor", entry.id)}
            aria-current={entry.id === tab ? "page" : undefined}
            onClick={(event) => {
              event.preventDefault();
              navigateTo(pathForLane("governor", entry.id));
            }}
            className={
              entry.id === tab
                ? "-mb-px border-b-2 border-zinc-900 pb-2 font-medium text-zinc-900 dark:border-white dark:text-white"
                : "pb-2 text-zinc-600 hover:text-zinc-900 dark:text-zinc-300 dark:hover:text-white"
            }
            title={entry.producer}
          >
            {entry.label}
          </a>
        ))}
      </nav>
      {tab === "brief" ? (brief ?? <BriefScreen />) : (scorecard ?? <ScorecardScreen />)}
    </div>
  );
}
