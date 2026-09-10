import { Component, useEffect, useState, type ErrorInfo, type ReactNode } from "react";
import type { MetaOutput } from "@rocky-types/meta";
import { ApiError, apiGet } from "./api";
import { EmptyState, StatusCard } from "./components";
import { EstateScreen } from "./estate/EstateScreen";
import { GovernorScreen } from "./governor/GovernorScreen";
import { ReviewScreen } from "./review/ReviewScreen";
import { LANES, navigate, pathForLane, useLane, type Lane } from "./router";
import { currentToken } from "./token";

interface ErrorBoundaryState {
  error: Error | undefined;
}

class ErrorBoundary extends Component<{ children: ReactNode }, ErrorBoundaryState> {
  state: ErrorBoundaryState = { error: undefined };

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error("Rocky UI crashed:", error, info.componentStack);
  }

  render(): ReactNode {
    if (this.state.error) {
      return (
        <div className="p-4 text-red-700 dark:text-red-300">
          <p className="font-semibold">This screen hit an error.</p>
          <pre className="mt-2 whitespace-pre-wrap text-sm">{this.state.error.message}</pre>
        </div>
      );
    }
    return this.props.children;
  }
}

type EngineState =
  | { kind: "loading" }
  | { kind: "no_token" }
  | { kind: "ready"; meta: MetaOutput }
  | { kind: "refused"; error: ApiError }
  | { kind: "unreachable"; message: string };

/**
 * The engine panel: `GET /api/v1/meta` with the tab's token. It proves the
 * whole path on every load: embedded assets, token bootstrap, bearer
 * header, typed payload, envelope on refusal.
 */
export function EnginePanel({
  fetchMeta = () => apiGet<MetaOutput>("meta"),
  token = currentToken(sessionStorage),
}: {
  fetchMeta?: () => Promise<MetaOutput>;
  token?: string | null;
}) {
  const [state, setState] = useState<EngineState>(
    token === null ? { kind: "no_token" } : { kind: "loading" },
  );

  useEffect(() => {
    if (token === null) return;
    let cancelled = false;
    fetchMeta()
      .then((meta) => {
        if (!cancelled) setState({ kind: "ready", meta });
      })
      .catch((error: unknown) => {
        if (cancelled) return;
        if (error instanceof ApiError) setState({ kind: "refused", error });
        else setState({ kind: "unreachable", message: String(error) });
      });
    return () => {
      cancelled = true;
    };
  }, [fetchMeta, token]);

  switch (state.kind) {
    case "no_token":
      return (
        <EmptyState
          title="No token for this tab"
          detail={
            <>
              Open the address <code>rocky serve --ui</code> printed; it carries the token in its
              fragment.
            </>
          }
        />
      );
    case "loading":
      return <p className="text-sm text-zinc-500">Reaching the engine…</p>;
    case "refused":
      return (
        <StatusCard
          label={`refused (${state.error.status})`}
          value={state.error.envelope.code}
          tone="risk"
          sub={state.error.envelope.remediation_hint ?? state.error.envelope.message}
        />
      );
    case "unreachable":
      return <StatusCard label="engine" value="unreachable" tone="risk" sub={state.message} />;
    case "ready": {
      const { meta } = state;
      const count = meta.capabilities.length;
      // One line, not three cards. It still proves the whole path works —
      // embedded assets, token bootstrap, bearer header, typed payload — but
      // it stops spending the top of every screen saying so. The full
      // capability list moves into a tooltip, the idiom the governor tabs
      // already use for their producer routes.
      return (
        <p className="text-xs text-zinc-600 dark:text-zinc-300">
          <span className="font-medium text-zinc-900 dark:text-zinc-100">
            rocky {meta.engine_version}
          </span>
          <Dot />
          state schema v{meta.state_schema_version}
          <Dot />
          <span title={meta.capabilities.join(", ")}>
            {count} {count === 1 ? "capability" : "capabilities"}
          </span>
          {/*
            A `title` is a mouse affordance: it is not in the tab order and
            screen-reader support for it is inconsistent. The names would
            otherwise be reachable by hover alone, so they are also here, off
            the page but in the accessibility tree.
          */}
          <span className="sr-only">: {meta.capabilities.join(", ")}</span>
        </p>
      );
    }
  }
}

/** The separator between the engine line's three facts. */
function Dot() {
  return <span className="px-1.5 text-zinc-400 dark:text-zinc-600">·</span>;
}

/**
 * What the page is when this tab holds no token.
 *
 * It is the whole page, not a banner above one: every lane's panels read the
 * API, and without a token each read is a refusal the viewer can do nothing
 * about. Four `REFUSED (401)` cards under a "no token" notice describe the
 * same single fact four times and read as a broken install.
 */
function NoToken() {
  return (
    <EmptyState
      title="No token for this tab"
      detail={
        <>
          Open the address <code>rocky serve --ui</code> printed. It carries the token in its
          fragment, which this page reads once and then clears from the address bar.
        </>
      }
    />
  );
}

/** What each lane shows. */
function LaneScreen({
  lane,
  estate,
  review,
  governor,
}: {
  lane: Lane;
  estate: ReactNode;
  review: ReactNode;
  governor: ReactNode;
}) {
  switch (lane) {
    case "estate":
      return <>{estate}</>;
    case "review":
      return <>{review}</>;
    case "governor":
      return <>{governor}</>;
  }
}

/**
 * The shell: the lane nav, the engine line, and the selected lane.
 *
 * The token check is **one boundary here**, above `LaneScreen`, rather than a
 * gate inside each lane. A lane cannot gate itself: returning after its
 * `useResource` calls is too late, the loads have already started, and
 * returning before them makes the hooks conditional. One boundary is also the
 * only shape that stays true when a fourth lane is added — a per-lane gate
 * would let that lane fire requests nobody notices.
 */
export function App({
  engine,
  estate,
  review,
  governor,
  token = currentToken(sessionStorage),
}: {
  engine?: ReactNode;
  estate?: ReactNode;
  review?: ReactNode;
  governor?: ReactNode;
  token?: string | null;
}) {
  const lane = useLane();
  return (
    <ErrorBoundary>
      <div className="min-h-screen bg-zinc-50 text-zinc-900 dark:bg-zinc-950 dark:text-zinc-100">
        <header className="border-b border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-900">
          <div className="mx-auto flex max-w-6xl items-center gap-6 px-4 py-3">
            <span className="text-base font-semibold tracking-tight">Rocky</span>
            <nav aria-label="Lanes" className="flex gap-4 text-sm">
              {LANES.map((entry) => (
                <a
                  key={entry.id}
                  href={pathForLane(entry.id)}
                  aria-current={entry.id === lane ? "page" : undefined}
                  onClick={(event) => {
                    event.preventDefault();
                    navigate(entry.id);
                  }}
                  className={
                    entry.id === lane
                      ? "font-medium text-zinc-900 dark:text-white"
                      : "text-zinc-600 hover:text-zinc-900 dark:text-zinc-300 dark:hover:text-white"
                  }
                >
                  {entry.label}
                </a>
              ))}
            </nav>
          </div>
        </header>
        <main className="mx-auto max-w-6xl space-y-4 px-4 py-6">
          {token === null ? (
            <NoToken />
          ) : (
            <>
              <section aria-label="Engine">{engine ?? <EnginePanel />}</section>
              <LaneScreen
                lane={lane}
                estate={estate ?? <EstateScreen />}
                review={review ?? <ReviewScreen />}
                governor={governor ?? <GovernorScreen />}
              />
            </>
          )}
        </main>
      </div>
    </ErrorBoundary>
  );
}
