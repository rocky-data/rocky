import { createContext } from "react";
import type { JobStore } from "./store";

/** The tab's job store, set by the shell. `null`: each control keeps its own. */
export const JobStoreContext = createContext<JobStore | null>(null);
