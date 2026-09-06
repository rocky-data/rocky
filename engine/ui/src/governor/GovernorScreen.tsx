import type { ReactNode } from "react";
import { navigateTo, pathForLane, useSegments } from "../router";
import { AuditScreen } from "./AuditScreen";
import { BriefScreen } from "./BriefScreen";
import { CustodyScreen } from "./CustodyScreen";
import { ProductsScreen } from "./ProductsScreen";
import { ScorecardScreen } from "./ScorecardScreen";

const TABS = [
  { id: "brief", label: "Brief", producer: "GET /api/v1/brief" },
  { id: "scorecard", label: "Scorecard", producer: "GET /api/v1/audit/scorecard" },
  { id: "custody", label: "Custody", producer: "GET /api/v1/custody/{subject}" },
  { id: "audit", label: "Audit", producer: "GET /api/v1/audit" },
  { id: "products", label: "Products", producer: "GET /api/v1/products/{name}/journal" },
] as const;

type Tab = (typeof TABS)[number]["id"];

function tabFromSegment(segment: string | undefined): Tab {
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

/**
 * The governor lane: the brief, the trust scorecard, the custody
 * drill-down, the audit browse and the product timelines, one tab each,
 * deep-linked at `/ui/governor/<screen>` (`/ui/governor/custody/<subject>`
 * for a subject, `/ui/governor/products/<name>` for one product).
 *
 * The product timeline lives here rather than in a lane of its own: it
 * answers the question the other four answer — what happened, and who decided
 * it — for the same reader.
 */
export function GovernorScreen({
  brief,
  scorecard,
  custody,
  audit,
  products,
}: {
  brief?: ReactNode;
  scorecard?: ReactNode;
  custody?: (subject: string | null) => ReactNode;
  audit?: ReactNode;
  products?: (name: string | null) => ReactNode;
}) {
  const segments = useSegments();
  const tab = tabFromSegment(segments[1]);
  const subject = tab === "custody" && segments[2] ? decodeURIComponent(segments[2]) : null;
  const productName = tab === "products" && segments[2] ? decodeURIComponent(segments[2]) : null;

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
    case "products":
      screen = products ? products(productName) : <ProductsScreen name={productName} />;
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
