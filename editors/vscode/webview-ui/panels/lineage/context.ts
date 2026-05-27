import { createContext, useContext } from "react";

/** Whether nodes are colored by resource kind or by materialization strategy. */
export type ColorMode = "kind" | "materialization";

export const ColorModeContext = createContext<ColorMode>("kind");

export function useColorMode(): ColorMode {
  return useContext(ColorModeContext);
}
