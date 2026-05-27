import {
  createContext,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";

/** Active VS Code theme kind, derived from the `<body>` class VS Code sets. */
export type ThemeKind = "light" | "dark" | "high-contrast" | "high-contrast-light";

function readThemeFromBody(): ThemeKind {
  const classes = document.body.classList;
  if (classes.contains("vscode-high-contrast-light")) return "high-contrast-light";
  if (classes.contains("vscode-high-contrast")) return "high-contrast";
  if (classes.contains("vscode-light")) return "light";
  return "dark";
}

const ThemeContext = createContext<ThemeKind>("dark");

/**
 * Exposes the active VS Code theme kind to descendants, tracking `<body>` class
 * changes (VS Code swaps the class on theme switch). Components style through
 * `--vscode-*` CSS variables, so most never read this — it exists for the rare
 * JS-side palette decision (e.g. the lineage minimap).
 */
export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setTheme] = useState<ThemeKind>(readThemeFromBody);

  useEffect(() => {
    const observer = new MutationObserver(() => setTheme(readThemeFromBody()));
    observer.observe(document.body, {
      attributes: true,
      attributeFilter: ["class"],
    });
    return () => observer.disconnect();
  }, []);

  return <ThemeContext.Provider value={theme}>{children}</ThemeContext.Provider>;
}

/** The active VS Code theme kind. */
export function useTheme(): ThemeKind {
  return useContext(ThemeContext);
}
