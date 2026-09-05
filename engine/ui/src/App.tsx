import { Component, useEffect, useState, type ErrorInfo, type ReactNode } from "react";
import type { MetaOutput } from "@rocky-types/meta";
import { ApiError, apiGet } from "./api";
import { EmptyState, StatusCard } from "./components";
import { currentToken } from "./token";

/** The three lanes the U-series builds. Only the shell exists in U2-P1. */
const LANES = [
  { id: "estate", label: "Estate" },
  { id: "review", label: "Review" },
  { id: "governor", label: "Governor" },
] as const;

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
 * The engine panel: `GET /api/v1/meta` with the tab's token. It is the one
 * screen of the scaffold, and it proves the whole path: embedded assets,
 * token bootstrap, bearer header, typed payload, envelope on refusal.
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
      return (
        <div className="grid gap-3 sm:grid-cols-3">
          <StatusCard label="engine" value={meta.engine_version} tone="ok" sub="rocky serve" />
          <StatusCard label="state schema" value={`v${meta.state_schema_version}`} />
          <StatusCard
            label="capabilities"
            value={meta.capabilities.length}
            sub={meta.capabilities.join(", ")}
          />
        </div>
      );
    }
  }
}

export function App({ engine }: { engine?: ReactNode }) {
  return (
    <ErrorBoundary>
      <div className="min-h-screen bg-zinc-50 text-zinc-900 dark:bg-zinc-950 dark:text-zinc-100">
        <header className="border-b border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-900">
          <div className="mx-auto flex max-w-6xl items-center gap-6 px-4 py-3">
            <span className="text-base font-semibold tracking-tight">Rocky</span>
            <nav aria-label="Lanes" className="flex gap-4 text-sm">
              {LANES.map((lane) => (
                <a
                  key={lane.id}
                  href={`/ui/${lane.id}`}
                  className="text-zinc-600 hover:text-zinc-900 dark:text-zinc-300 dark:hover:text-white"
                >
                  {lane.label}
                </a>
              ))}
            </nav>
          </div>
        </header>
        <main className="mx-auto max-w-6xl px-4 py-6">
          <h1 className="mb-3 text-sm font-medium uppercase tracking-wide text-zinc-500">
            Engine
          </h1>
          {engine ?? <EnginePanel />}
        </main>
      </div>
    </ErrorBoundary>
  );
}
