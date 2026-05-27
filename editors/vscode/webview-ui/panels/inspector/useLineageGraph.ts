import { useEffect, useMemo, useState } from "react";
import type {
  BreakingData,
  DriftData,
  GovernanceData,
  GraphData,
  ReplayData,
} from "../../../src/webviews/lineage/contract";
import { getRpc } from "../../runtime/rpcClient";
import { type ColorMode, type OverlayKind } from "./context";
import { makeBreakingOverlay } from "./overlays/breaking";
import { costOverlay } from "./overlays/cost";
import { makeDriftOverlay } from "./overlays/drift";
import { freshnessOverlay } from "./overlays/freshness";
import { makeGovernanceOverlay } from "./overlays/governance";
import { makeLastRunOverlay } from "./overlays/lastRun";
import type { LineageOverlay } from "./overlays/types";

/**
 * Owns the project lineage graph and the trust-plane overlay state: requests
 * the graph once, tracks which overlays are active, lazily fetches the data an
 * overlay needs the first time it is enabled, and composes the active overlays
 * into the list the canvas decorates with. Shared by the Inspector's Lineage
 * tab and the standalone lineage view so neither duplicates the wiring.
 */
export function useLineageGraph() {
  const [graph, setGraph] = useState<GraphData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [colorMode, setColorMode] = useState<ColorMode>("kind");
  const [search, setSearch] = useState("");
  const [active, setActive] = useState<Set<OverlayKind>>(new Set());
  const [drift, setDrift] = useState<DriftData | null>(null);
  const [breaking, setBreaking] = useState<BreakingData | null>(null);
  const [replay, setReplay] = useState<ReplayData | null>(null);
  const [governance, setGovernance] = useState<GovernanceData | null>(null);

  useEffect(() => {
    void getRpc()
      .request<GraphData>("graph")
      .then(setGraph)
      .catch((err) => setError(String(err)));
  }, []);

  const toggleOverlay = (kind: OverlayKind): void => {
    setActive((prev) => {
      const next = new Set(prev);
      if (next.has(kind)) next.delete(kind);
      else next.add(kind);
      return next;
    });
    // Each data-backed overlay fetches on first enable; the base graph is unaffected.
    if (kind === "drift" && drift === null) {
      void getRpc()
        .request<DriftData>("drift")
        .then(setDrift)
        .catch((err) => setDrift({ actions: [], unavailable: String(err) }));
    }
    if (kind === "breaking" && breaking === null) {
      void getRpc()
        .request<BreakingData>("breaking")
        .then(setBreaking)
        .catch((err) =>
          setBreaking({ baseRef: "main", findings: [], unavailable: String(err) }),
        );
    }
    if (kind === "lastRun" && replay === null) {
      void getRpc()
        .request<ReplayData>("replay")
        .then(setReplay)
        .catch((err) => setReplay({ models: [], unavailable: String(err) }));
    }
    if (kind === "governance" && governance === null) {
      void getRpc()
        .request<GovernanceData>("governance")
        .then(setGovernance)
        .catch((err) => setGovernance({ models: [], unavailable: String(err) }));
    }
  };

  const overlays = useMemo<LineageOverlay[]>(() => {
    const list: LineageOverlay[] = [];
    if (active.has("cost")) list.push(costOverlay);
    if (active.has("freshness")) list.push(freshnessOverlay);
    if (active.has("drift") && drift) list.push(makeDriftOverlay(drift));
    if (active.has("breaking") && breaking && graph) {
      list.push(makeBreakingOverlay(breaking, graph));
    }
    if (active.has("lastRun") && replay) list.push(makeLastRunOverlay(replay));
    if (active.has("governance") && governance) {
      list.push(makeGovernanceOverlay(governance));
    }
    return list;
  }, [active, drift, breaking, replay, governance, graph]);

  return {
    graph,
    error,
    colorMode,
    setColorMode,
    search,
    setSearch,
    active,
    toggleOverlay,
    overlays,
  };
}
