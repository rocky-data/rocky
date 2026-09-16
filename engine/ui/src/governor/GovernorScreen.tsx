import type { ReactNode } from "react";
import { navigateTo, pathForLane, useSegments } from "../router";
import { AuditScreen } from "./AuditScreen";
import { BriefScreen } from "./BriefScreen";
import { CustodyScreen } from "./CustodyScreen";
import { ProductsScreen } from "./ProductsScreen";
import { ScorecardScreen } from "./ScorecardScreen";
import { GOVERNOR_TABS, governorAreaOf, governorTabFromSegment } from "./tabs";

/**
 * The governor lane: the brief, the trust scorecard, the custody
 * drill-down, the audit browse and the product timelines, deep-linked at
 * `/ui/governor/<screen>` (`/ui/governor/custody/<subject>` for a subject,
 * `/ui/governor/products/<name>` for one product,
 * `/ui/governor/audit/<product>` for the ledger scoped to one product).
 *
 * The shell's sidebar gives the brief and the product timelines areas of
 * their own (Needs you, Products), so the tab bar shows only the tabs of the
 * current area: Scorecard, Custody and Audit under Governance, and no bar at
 * all for an area of one screen. A tab that was also a sidebar entry would be
 * two navigations marking the same page current.
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
  audit?: (product: string | null) => ReactNode;
  products?: (name: string | null) => ReactNode;
}) {
  const segments = useSegments();
  const tab = governorTabFromSegment(segments[1]);
  const area = governorAreaOf(tab);
  const tabs = GOVERNOR_TABS.filter((entry) => entry.area === area);
  const subject = tab === "custody" && segments[2] ? decodeURIComponent(segments[2]) : null;
  const productName = tab === "products" && segments[2] ? decodeURIComponent(segments[2]) : null;
  const auditProduct = tab === "audit" && segments[2] ? decodeURIComponent(segments[2]) : null;

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
      screen = audit ? audit(auditProduct) : <AuditScreen product={auditProduct} />;
      break;
    case "products":
      screen = products ? products(productName) : <ProductsScreen name={productName} />;
      break;
  }

  return (
    <div className="space-y-4">
      {/*
        `flex-wrap`, because five tabs did not fit 320px: measured, the row ran
        3–4px past the page body and took the whole page sideways with it.
        Three fit today; wrapping still costs nothing and keeps every tab
        reachable if one is added, which a horizontal page scroll does not.
      */}
      {tabs.length > 1 && (
        <nav
          aria-label="Governor screens"
          className="flex flex-wrap gap-x-4 gap-y-1 border-b border-zinc-200 text-sm dark:border-zinc-800"
        >
          {tabs.map((entry) => (
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
      )}
      {screen}
    </div>
  );
}
