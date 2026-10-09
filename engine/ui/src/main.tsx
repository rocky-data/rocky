import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./styles/base.css";
import { App } from "./App";

// No token handling here: `GET /login?t=…` set an `HttpOnly` session cookie
// before this page loaded, and every API call carries it (`api.ts`).
const container = document.getElementById("root");
if (!container) {
  throw new Error("index.html has no #root element");
}
createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
