import {
  Component,
  useEffect,
  useId,
  useState,
  type ErrorInfo,
  type ReactNode,
} from "react";
import { Dialog, DialogBackdrop, DialogPanel, useClose } from "@headlessui/react";
import { Bars3Icon, XMarkIcon } from "@heroicons/react/24/outline";
import type { MetaOutput } from "@rocky-types/meta";
import { ApiError, apiGet } from "./api";
import {
  AREAS,
  NOT_YET_HEADING,
  areaFromPath,
  areaHasTabs,
  type Area,
  type AreaId,
} from "./areas";
import markUrl from "./assets/rocky-logo.svg";
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
 * The production reader, at module scope so its identity never changes.
 *
 * As a default parameter (`fetchMeta = () => apiGet(...)`) it was a NEW
 * function on every render, and the effect below depends on it: the effect
 * fetched, the answer set state, the state rendered, the render made another
 * function, and the effect ran again. An idle tab asked the engine for
 * `/api/v1/meta` about 15 times a second, for as long as it was open (#2075).
 */
const fetchMetaFromEngine = (): Promise<MetaOutput> => apiGet<MetaOutput>("meta");

/**
 * The engine read, as a hook, so the shell can own it.
 *
 * The sidebar is drawn twice below the breakpoint — once fixed, once in the
 * drawer — and two mounted panels would be two reads of the same route. The
 * shell calls this once and hands the state to each copy.
 */
export function useEngineMeta(
  token: string | null,
  fetchMeta: () => Promise<MetaOutput> = fetchMetaFromEngine,
): EngineState {
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

  return state;
}

/**
 * The engine panel: `GET /api/v1/meta` with the tab's token. It proves the
 * whole path on every load: embedded assets, token bootstrap, bearer
 * header, typed payload, envelope on refusal.
 */
export function EnginePanel({
  fetchMeta = fetchMetaFromEngine,
  token = currentToken(sessionStorage),
}: {
  fetchMeta?: () => Promise<MetaOutput>;
  token?: string | null;
}) {
  return <EngineLine state={useEngineMeta(token, fetchMeta)} />;
}

/** One engine state, drawn. Owns no read, so it can be drawn twice. */
export function EngineLine({ state }: { state: EngineState }) {
  // Unique per copy: two lines in one page must not share the id their
  // `aria-describedby` points at.
  const capabilitiesId = useId();

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
          <span title={capabilities} aria-describedby={capabilitiesId}>
            {count} {count === 1 ? "capability" : "capabilities"}
          </span>
          <span id={capabilitiesId} hidden>
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

/**
 * Where the fixed sidebar takes over from the drawer: Tailwind's `lg`, which
 * the classes below spell as `lg:`. Kept here as the one place the number is
 * written, because the shell has to close the drawer when a resize crosses it.
 */
export const WIDE_ENOUGH_FOR_THE_SIDEBAR = "(min-width: 64rem)";

/**
 * The mark and the name. The mark is the same file the docs site serves as
 * its favicon, bundled with the page — nothing loads from another host.
 *
 * `alt=""`, because the mark says nothing the name beside it does not: a
 * screen reader that announced both would say "Rocky" twice. The caller
 * passes the display class, so each copy of the sidebar sets its own.
 */
function Wordmark({ className }: { className: string }) {
  return (
    <span className={`items-center gap-2 text-base font-semibold tracking-tight ${className}`}>
      {/* No CSS rounding: the mark draws its own rounded tile (`rx="36"` on a
          200-wide square), so a radius here would clip it at a different
          curve than the docs favicon shows. */}
      <img src={markUrl} alt="" width={28} height={28} />
      Rocky
    </span>
  );
}

/**
 * The eleven areas. A link for an area with a screen; for one without, its
 * name and the reason as plain text — not a link, not in the tab order, and
 * marked disabled for assistive technology.
 */
function AreaLink({ area, current }: { area: Area & { kind: "link" }; current: AreaId }) {
  const here = area.id === current;
  const Icon = area.icon;
  // Folds the drawer this link is drawn in. Outside a dialog — the fixed rail
  // — the default context is a no-op, so the same link works in both copies.
  const close = useClose();
  return (
    <a
      href={area.href}
      // "page" unless the area's screen has tabs of its own; then the tab is
      // the page and this is the section it is in.
      aria-current={here ? (areaHasTabs(area.id) ? "true" : "page") : undefined}
      onClick={(event) => {
        event.preventDefault();
        navigateTo(area.href);
        // Not left to the route change: tapping the area you are already on
        // pushes the same path, so `usePathname` sets an identical string,
        // React bails out, and the effect keyed on it never runs. The drawer
        // would stay open over the page it just failed to navigate away from.
        close();
      }}
      className={`group flex gap-x-3 rounded-md p-2 text-sm/6 font-semibold ${
        here
          ? "bg-zinc-100 text-zinc-900 dark:bg-white/5 dark:text-white"
          : "text-zinc-700 hover:bg-zinc-100 hover:text-zinc-900 dark:text-zinc-400 dark:hover:bg-white/5 dark:hover:text-white"
      }`}
    >
      <Icon
        aria-hidden="true"
        className={`size-6 shrink-0 ${
          here
            ? "text-zinc-900 dark:text-white"
            : "text-zinc-400 group-hover:text-zinc-900 dark:group-hover:text-white"
        }`}
      />
      {area.label}
    </a>
  );
}

/**
 * The eleven areas: first the five that open a screen, then the six that do
 * not, under a heading, each with its reason as plain text. A disabled entry
 * is not a link — no tab stop — and says so to assistive technology.
 */
function AreaNav({ current }: { current: AreaId }) {
  const open = AREAS.filter((area) => area.kind === "link");
  const notYet = AREAS.filter((area) => area.kind === "disabled");
  return (
    <nav aria-label="Areas" className="flex flex-1 flex-col">
      <ul className="flex flex-1 flex-col gap-y-7">
        <li>
          <ul className="-mx-2 space-y-1">
            {open.map((area) => (
              <li key={area.id}>
                <AreaLink area={area as Area & { kind: "link" }} current={current} />
              </li>
            ))}
          </ul>
        </li>
        <li>
          <div className="text-xs/6 font-semibold text-zinc-500 dark:text-zinc-400">
            {NOT_YET_HEADING}
          </div>
          <ul className="-mx-2 mt-2 space-y-1">
            {notYet.map((area) => (
              <li key={area.id}>
                <span
                  aria-disabled="true"
                  className="flex gap-x-3 rounded-md p-2 text-sm/6 text-zinc-400 dark:text-zinc-500"
                >
                  <area.icon aria-hidden="true" className="size-6 shrink-0 text-zinc-300 dark:text-zinc-600" />
                  <span>
                    <span className="font-semibold">{area.label}</span>
                    <span className="block text-xs text-zinc-500 dark:text-zinc-400">
                      {area.kind === "disabled" ? area.reason : ""}
                    </span>
                  </span>
                </span>
              </li>
            ))}
          </ul>
        </li>
      </ul>
    </nav>
  );
}

/**
 * What the sidebar holds, drawn once in the fixed rail and once in the
 * drawer — never both at the same time: the drawer exists only while it is
 * open, and above the breakpoint it closes itself.
 */
function SidebarContents({ current, engine }: { current: AreaId; engine: ReactNode }) {
  return (
    <div className="flex grow flex-col gap-y-5 overflow-y-auto border-r border-zinc-200 bg-white px-6 dark:border-white/10 dark:bg-zinc-900">
      <div className="flex h-16 shrink-0 items-center">
        <Wordmark className="flex" />
      </div>
      <AreaNav current={current} />
      {engine !== null && (
        <section
          aria-label="Engine"
          className="-mx-6 mt-auto border-t border-zinc-200 px-6 py-3 dark:border-white/10"
        >
          {engine}
        </section>
      )}
    </div>
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
 * Below `lg` the sidebar folds behind an Areas button and opens as a
 * slide-over. While the drawer is open the areas are in the page twice, but
 * only once for anyone reading it: the dialog marks the rest of the page
 * `inert` and `aria-hidden`, so the rail's copy offers no tab stop and no
 * second current link. It closes on every navigation, on a click that does
 * not navigate — the area you are already on — and when the viewport grows
 * past the breakpoint, where an open drawer would keep its focus trap over
 * the fixed sidebar. Escape and focus return are the dialog's own.
 *
 * The engine read is the shell's, not the sidebar's, so the drawer and the
 * rail draw the same one read.
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
  // One read for both copies of the sidebar, and none without a token.
  const engineState = useEngineMeta(token);
  const engineLine = token === null ? null : (engine ?? <EngineLine state={engineState} />);
  const areaLabel = AREAS.find((entry) => entry.id === area)?.label ?? "Rocky";

  // Fold on any route change, Back and Forward included, not only on a click.
  useEffect(() => setMenuOpen(false), [pathname]);

  // And when the viewport grows past the breakpoint, where the drawer is
  // hidden but would keep its focus trap and scroll lock over the page.
  useEffect(() => {
    if (typeof window.matchMedia !== "function") return;
    const wide = window.matchMedia(WIDE_ENOUGH_FOR_THE_SIDEBAR);
    const onChange = (event: MediaQueryListEvent) => {
      if (event.matches) setMenuOpen(false);
    };
    wide.addEventListener("change", onChange);
    return () => wide.removeEventListener("change", onChange);
  }, []);

  return (
    <ErrorBoundary>
      <div className="min-h-screen bg-zinc-50 text-zinc-900 dark:bg-zinc-950 dark:text-zinc-100">
        {/* The drawer, below `lg`. Mounted only while open. */}
        <Dialog
          open={menuOpen}
          onClose={setMenuOpen}
          aria-label="Areas"
          className="relative z-50 lg:hidden"
        >
          <DialogBackdrop
            transition
            className="fixed inset-0 bg-zinc-900/80 transition-opacity duration-300 ease-linear data-closed:opacity-0"
          />
          <div className="fixed inset-0 flex">
            <DialogPanel
              transition
              className="relative mr-16 flex w-full max-w-xs flex-1 transform transition duration-300 ease-in-out data-closed:-translate-x-full"
            >
              <div className="absolute top-0 left-full flex w-16 justify-center pt-5">
                <button type="button" onClick={() => setMenuOpen(false)} className="-m-2.5 p-2.5">
                  <span className="sr-only">Close the areas</span>
                  <XMarkIcon aria-hidden="true" className="size-6 text-white" />
                </button>
              </div>
              <SidebarContents current={area} engine={engineLine} />
            </DialogPanel>
          </div>
        </Dialog>

        {/* The fixed sidebar, `lg` and up. */}
        <div className="hidden lg:fixed lg:inset-y-0 lg:z-50 lg:flex lg:w-72 lg:flex-col">
          <SidebarContents current={area} engine={engineLine} />
        </div>

        <div className="sticky top-0 z-40 flex items-center gap-x-6 border-b border-zinc-200 bg-white px-4 py-4 sm:px-6 lg:hidden dark:border-white/10 dark:bg-zinc-900">
          <button
            type="button"
            aria-expanded={menuOpen}
            aria-haspopup="dialog"
            onClick={() => setMenuOpen(true)}
            className="-m-2.5 p-2.5 text-zinc-700 hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-white"
          >
            <span className="sr-only">Areas</span>
            <Bars3Icon aria-hidden="true" className="size-6" />
          </button>
          <div className="flex-1 text-sm/6 font-semibold text-zinc-900 dark:text-white">
            {areaLabel}
          </div>
          <Wordmark className="flex" />
        </div>

        <main className="py-10 lg:pl-72">
          <div className="mx-auto max-w-6xl space-y-4 px-4 sm:px-6 lg:px-8">
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
          </div>
        </main>
      </div>
    </ErrorBoundary>
  );
}
