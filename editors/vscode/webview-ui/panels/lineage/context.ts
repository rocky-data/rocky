import { createContext, useContext } from "react";
import type { LineageOverlay } from "./overlays/types";

/** Whether nodes are colored by resource kind or by materialization strategy. */
export type ColorMode = "kind" | "materialization";

export const ColorModeContext = createContext<ColorMode>("kind");

export function useColorMode(): ColorMode {
  return useContext(ColorModeContext);
}

/** Overlay toggles available on the canvas. */
export type OverlayKind =
  | "cost"
  | "freshness"
  | "drift"
  | "breaking"
  | "lastRun"
  | "governance";

/** The overlays currently decorating nodes (cost, freshness, drift, …). */
export const OverlaysContext = createContext<LineageOverlay[]>([]);

export function useOverlays(): LineageOverlay[] {
  return useContext(OverlaysContext);
}
