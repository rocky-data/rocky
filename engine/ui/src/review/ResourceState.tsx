import type { ReactNode } from "react";
import { StatusCard } from "../components";
import type { Resource } from "../estate/useResource";

/**
 * What a panel shows while its producer has not answered, or has refused.
 *
 * A refusal is a state, not an exception: the engine's own `code` and
 * remediation hint are rendered, never flattened into "something went wrong".
 * The review lane leans on this hard — a sample refused because the adapter
 * cannot express a mask is a different thing from one refused because nobody
 * consented, and a reviewer must be able to tell them apart.
 */
export function ResourceState<T>({
  resource,
  loadingLine,
}: {
  resource: Resource<T>;
  loadingLine: string;
}): ReactNode {
  switch (resource.kind) {
    case "loading":
      return <p className="text-sm text-zinc-500 dark:text-zinc-400">{loadingLine}</p>;
    case "refused":
      return (
        <StatusCard
          label={`refused (${resource.error.status})`}
          value={resource.error.envelope.code}
          tone="risk"
          sub={resource.error.envelope.remediation_hint ?? resource.error.envelope.message}
        />
      );
    case "unreachable":
      return (
        <StatusCard
          label="unreachable"
          value="the engine did not answer"
          tone="risk"
          sub={resource.message}
        />
      );
    case "ready":
      return null;
  }
}
