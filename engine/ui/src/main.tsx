import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./styles/base.css";
import { App } from "./App";
import { bootstrapToken } from "./token";

// The token arrives once, in the URL fragment of the address `rocky serve
// --ui` prints. Read it before the first render, so the first API call
// already carries it, and clear it from the address bar.
bootstrapToken(window);

const container = document.getElementById("root");
if (!container) {
  throw new Error("index.html has no #root element");
}
createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
