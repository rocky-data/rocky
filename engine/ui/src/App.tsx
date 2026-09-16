import {
  Component,
  useEffect,
  useRef,
  useState,
  type ErrorInfo,
  type ReactNode,
} from "react";
import type { MetaOutput } from "@rocky-types/meta";
import { ApiError, apiGet } from "./api";
import { AREAS, areaFromPath, type AreaId } from "./areas";
import { EmptyState, StatusCard } from "./components";
import { EstateScreen } from "./estate/EstateScreen";
import { GovernorScreen } from "./governor/GovernorScreen";
import { ReviewScreen } from "./review/ReviewScreen";
import { laneFromPath, navigateTo, usePathname, type Lane } from "./router";
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
      const capabilities = meta.capabilities.join(", ");
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
          {/*
            The names reach a mouse through `title` and assistive technology
            through `aria-describedby`. The description element is `hidden`,
            so it is not a second node in the reading order: an otherwise
            unused `title` already becomes the accessible description, and an
            `sr-only` sibling would then be announced twice.
          */}
          <span title={capabilities} aria-describedby="engine-capabilities">
            {count} {count === 1 ? "capability" : "capabilities"}
          </span>
          <span id="engine-capabilities" hidden>
            {capabilities}
          </span>
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

/** The id the menu button controls. One sidebar exists, at every width. */
const SIDEBAR_ID = "shell-sidebar";

/**
 * The eleven areas. A link for an area with a screen; for one without, its
 * name and the reason as plain text — not a link, not in the tab order, and
 * marked disabled for assistive technology.
 */
function AreaNav({ current, onNavigate }: { current: AreaId; onNavigate: () => void }) {
  return (
    <nav aria-label="Areas">
      <ul className="space-y-0.5 text-sm">
        {AREAS.map((area) => (
          <li key={area.id}>
            {area.kind === "link" ? (
              <a
                href={area.href}
                aria-current={area.id === current ? "page" : undefined}
                onClick={(event) => {
                  event.preventDefault();
                  navigateTo(area.href);
                  onNavigate();
                }}
                className={`block rounded px-2 py-1.5 ${
                  area.id === current
                    ? "bg-zinc-100 font-medium text-zinc-900 dark:bg-zinc-800 dark:text-white"
                    : "text-zinc-700 hover:bg-zinc-100 hover:text-zinc-900 dark:text-zinc-300 dark:hover:bg-zinc-800 dark:hover:text-white"
                }`}
              >
                {area.label}
              </a>
            ) : (
              <span aria-disabled="true" className="block px-2 py-1.5 text-zinc-400 dark:text-zinc-500">
                {area.label}
                <span className="block text-xs text-zinc-500 dark:text-zinc-400">{area.reason}</span>
              </span>
            )}
          </li>
        ))}
      </ul>
    </nav>
  );
}

/**
 * The shell: the sidebar of areas, the engine line in its footer, and the
 * selected lane.
 *
 * The token check is **one boundary here**, above `LaneScreen` and the engine
 * line, rather than a gate inside each lane. A lane cannot gate itself:
 * returning after its `useResource` calls is too late, the loads have already
 * started, and returning before them makes the hooks conditional. One boundary
 * is also the only shape that stays true when a lane is added — a per-lane
 * gate would let that lane fire requests nobody notices.
 *
 * On a narrow screen the sidebar is folded behind a menu button. It is the
 * same element at every width, shown or hidden by CSS, so the engine line is
 * fetched once and its ids stay unique. It folds again on every navigation and
 * on Escape, which returns focus to the button.
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
  const pathname = usePathname();
  const lane: Lane = laneFromPath(pathname);
  const area = areaFromPath(pathname);
  const [menuOpen, setMenuOpen] = useState(false);
  const menuButton = useRef<HTMLButtonElement | null>(null);

  // Fold on any route change, Back and Forward included, not only on a click.
  useEffect(() => setMenuOpen(false), [pathname]);

  useEffect(() => {
    if (!menuOpen) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setMenuOpen(false);
      menuButton.current?.focus();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [menuOpen]);

  return (
    <ErrorBoundary>
      <div className="min-h-screen bg-zinc-50 text-zinc-900 md:flex dark:bg-zinc-950 dark:text-zinc-100">
        <header className="flex items-center justify-between border-b border-zinc-200 bg-white px-4 py-3 md:hidden dark:border-zinc-800 dark:bg-zinc-900">
          <span className="text-base font-semibold tracking-tight">Rocky</span>
          <button
            ref={menuButton}
            type="button"
            aria-expanded={menuOpen}
            aria-controls={SIDEBAR_ID}
            onClick={() => setMenuOpen((open) => !open)}
            className="rounded border border-zinc-300 px-2 py-1 text-xs text-zinc-700 hover:bg-zinc-100 dark:border-zinc-700 dark:text-zinc-200 dark:hover:bg-zinc-800"
          >
            Menu
          </button>
        </header>
        <aside
          id={SIDEBAR_ID}
          className={`${menuOpen ? "flex" : "hidden"} flex-col gap-4 border-b border-zinc-200 bg-white px-3 py-4 md:sticky md:top-0 md:flex md:h-screen md:w-60 md:shrink-0 md:overflow-y-auto md:border-r md:border-b-0 dark:border-zinc-800 dark:bg-zinc-900`}
        >
          <span className="hidden px-2 text-base font-semibold tracking-tight md:block">Rocky</span>
          <AreaNav current={area} onNavigate={() => setMenuOpen(false)} />
          {token !== null && (
            <section aria-label="Engine" className="mt-auto border-t border-zinc-200 px-2 pt-3 dark:border-zinc-800">
              {engine ?? <EnginePanel />}
            </section>
          )}
        </aside>
        <main className="mx-auto w-full max-w-6xl min-w-0 space-y-4 px-4 py-6">
          {token === null ? (
            <NoToken />
          ) : (
            <LaneScreen
              lane={lane}
              estate={estate ?? <EstateScreen />}
              review={review ?? <ReviewScreen />}
              governor={governor ?? <GovernorScreen />}
            />
          )}
        </main>
      </div>
    </ErrorBoundary>
  );
}
