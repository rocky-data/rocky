/**
 * Tailwind config for Rocky's React webviews.
 *
 * Scoped to `webview-ui/` only (content glob is relative to the npm cwd, which
 * is `editors/vscode/`). Preflight is OFF so Tailwind never resets the body
 * color/background/links that VS Code's theme owns — `styles/base.css` carries
 * the one minimal reset instead. Colors are aliased to `--vscode-*` CSS
 * variables so utilities like `text-vscode-fg` / `bg-vscode-bg` track the
 * user's active theme automatically.
 *
 * @type {import('tailwindcss').Config}
 */
module.exports = {
  content: ["./webview-ui/**/*.{ts,tsx}"],
  corePlugins: { preflight: false },
  theme: {
    extend: {
      colors: {
        "vscode-fg": "var(--vscode-foreground)",
        "vscode-bg": "var(--vscode-editor-background)",
        "vscode-widget-bg": "var(--vscode-editorWidget-background)",
        "vscode-border": "var(--vscode-panel-border)",
        "vscode-focus": "var(--vscode-focusBorder)",
        "vscode-desc": "var(--vscode-descriptionForeground)",
        "vscode-error": "var(--vscode-errorForeground)",
        "vscode-button-bg": "var(--vscode-button-background)",
        "vscode-button-fg": "var(--vscode-button-foreground)",
        "vscode-button-hover": "var(--vscode-button-hoverBackground)",
        "vscode-badge-bg": "var(--vscode-badge-background)",
        "vscode-badge-fg": "var(--vscode-badge-foreground)",
        "vscode-link": "var(--vscode-textLink-foreground)",
      },
    },
  },
  plugins: [],
};
