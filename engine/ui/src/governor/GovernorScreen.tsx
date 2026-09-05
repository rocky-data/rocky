import type { ReactNode } from "react";
import { navigateTo, pathForLane, useSegments } from "../router";
import { AuditScreen } from "./AuditScreen";
import { BriefScreen } from "./BriefScreen";
import { CustodyScreen } from "./CustodyScreen";
import { ScorecardScreen } from "./ScorecardScreen";

const TABS = [
  { id: "brief", label: "Brief", producer: "GET /api/v1/brief" },
  { id: "scorecard", label: "Scorecard", producer: "GET /api/v1/audit/scorecard" },
  { id: "custody", label: "Custody", producer: "GET /api/v1/custody/{subject}" },
  { id: "audit", label: "Audit", producer: "GET /api/v1/audit" },
] as const;

type Tab = (typeof TABS)[number]["id"];

function tabFromSegment(segment: string | undefined): Tab {
  return segment === "scorecard" || segment === "custody" || segment === "audit" ? segment : "brief";
}

/**
 * The governor lane: the brief, the trust scorecard, the custody
 * drill-down and the audit browse, one tab each, deep-linked at
 * `/ui/governor/<screen>` (`/ui/governor/custody/<subject>` for a subject).
 */
export function GovernorScreen({
  brief,
  scorecard,
  custody,
  audit,
}: {
  brief?: ReactNode;
  scorecard?: ReactNode;
  custody?: (subject: string | null) => ReactNode;
  audit?: ReactNode;
}) {
  const segments = useSegments();
  const tab = tabFromSegment(segments[1]);
  const subject = tab === "custody" && segments[2] ? decodeURIComponent(segments[2]) : null;

  let screen: ReactNode;
  switch (tab) {
    case "brief":
      screen = brief ?? <BriefScreen />;
      break;
    case "scorecard":
      screen = scorecard ?? <ScorecardScreen />;
      break;
    case "custody":
      screen = custody ? custody(subject) : <CustodyScreen subject={subject} />;
      break;
    case "audit":
      screen = audit ?? <AuditScreen />;
      break;
  }

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
      {screen}
    </div>
  );
}
