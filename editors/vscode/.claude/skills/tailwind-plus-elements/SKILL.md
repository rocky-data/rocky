---
name: tailwind-plus-elements
description: Using TailwindPlus Elements (@tailwindplus/elements) in the Rocky VS Code webviews. Use when building or upgrading an interactive webview component — a command palette / model search, a confirmation dialog, an autocomplete or select model picker, a dropdown/popover menu, a collapsible disclosure, or tabs. Covers the esbuild-bundle install (no CSP change vs the CDN), the --vscode-* theming + Tailwind v4 data-variant nuance, React 19 custom-element interop (boolean attrs, native events via ref, the elements:ready gate), and which element fits which Rocky surface.
---

# TailwindPlus Elements in Rocky webviews

[TailwindPlus Elements](https://tailwindcss.com/plus) (`@tailwindplus/elements`) is a framework-agnostic library of **headless web components** — `<el-dialog>`, `<el-command-palette>`, `<el-autocomplete>`, `<el-select>`, `<el-dropdown>`, `<el-popover>`, `<el-disclosure>`, `<el-tabs>` — that provide accessible, keyboard-friendly interactive behavior with **no styling of their own**. You style them with Tailwind. We hold a Tailwind Plus license; the full per-component API lives in the TailwindPlus account, not in this repo (see "Licensing" below). They slot into the React + Tailwind webviews under `editors/vscode/webview-ui/`.

## When to use this skill

- Building a new interactive webview surface: a `Cmd+K` command palette / model search, a confirmation dialog (e.g. the apply-gate), an autocomplete/combobox model picker, a dropdown or popover menu, a collapsible section, or tabs.
- Upgrading a hand-rolled webview interaction (e.g. the lineage canvas context menu) to an accessible primitive.
- **Not** for the ReactFlow canvas itself (that's `@xyflow/react`), and not a reason to re-skin a surface that already works.

## Install + bundling (no CSP change)

Add it as a devDependency in `editors/vscode` and import it **once** from a webview entry — esbuild bundles it into the panel chunk, so it loads under our existing nonce:

```bash
npm install -D @tailwindplus/elements   # in editors/vscode
```
```ts
// webview-ui/panels/<panel>/main.tsx — once per entry that uses Elements
import "@tailwindplus/elements";
```

Do **not** add the jsDelivr `<script src="…/@tailwindplus/elements">` CDN tag the official docs show: that would force a `script-src https://cdn.jsdelivr.net` into the webview CSP (`src/webviews/host/html.ts`) we don't otherwise need. Bundling via the import keeps everything under the nonce. The import registers the `<el-*>` custom elements at module-eval, before React mounts.

## Theming — keep the `--vscode-*` discipline

The elements paint no palette of their own, so theming is just our normal classes. Style the `<el-*>` wrappers and the native `<dialog>`/`<input>`/`<button>` they slot with the `--vscode-*`-aliased Tailwind classes from `webview-ui/styles/base.css`. Preflight is **off**, so the base.css `<button>` + border reset already applies to the native elements inside — theme them like any other control.

Transitions use read-only `data-closed` / `data-enter` / `data-leave` / `data-transition` attributes. We are on **Tailwind v4** (`webview-ui/styles/base.css` is the v4 entry). The arbitrary-variant form — `data-[closed]:opacity-0`, `transition data-[closed]:scale-95` — works; v4 also accepts the `data-closed:` boolean-attribute shorthand the official docs use. No webview exercises these yet, so verify against the current v4 docs when you add the first transition.

## React 19 interop

- **Use the typed React wrappers**, not raw `<el-*>` tags: `import { ElDialog, ElDialogPanel, ElDialogBackdrop, ElCommandPalette, ElCommandList, ElDisclosure, ElSelect, ElDropdown, … } from "@tailwindplus/elements/react"`. They're typed (`HTMLAttributes`), so no hand-rolled JSX intrinsics. Also add a static `import "@tailwindplus/elements"` in the webview entry (`main.tsx`) for **eager** registration — the wrappers otherwise lazy-import the base lib on mount via a runtime dynamic chunk.
- **Open/close** — drive from React state via the element's imperative methods behind a `ref`: `ref.current.show()` / `.hide()` on `<el-dialog>`, gated on `customElements.whenDefined("el-dialog")`. The declarative `open` / `hidden` attributes also work but aren't on the wrapper's `HTMLAttributes` type (so you'd cast); the Invoker Commands API (`command="show-modal" commandfor`) works for button triggers.
- **Events** (`change`, `open`, `close`, `cancel`, …) are native `CustomEvent`s, not React synthetic — wire via a `ref` + `addEventListener` in a `useEffect`, not `onChange`. Sync React state back on `close` / `cancel` so it doesn't desync; `cancel` is `preventDefault()`-able.
- **Don't let React re-apply attributes the element manages.** The command palette toggles `hidden` on its option buttons to filter; if React re-renders that list it fights the palette. Keep the options stable (memoize them, stable callbacks) so React doesn't reconcile them after the element has filtered.
- **Focus on open**: React's `autoFocus` fires at mount (while the dialog is still hidden), so focus the input explicitly via a `ref` when you open the dialog.
- **Imperative** — instances also expose `toggle()` / `reset()` / `setFilterCallback(cb)`.

## Which element fits which Rocky surface

| Element | Rocky use |
|---|---|
| `el-command-palette` (inside `el-dialog`) | `Cmd+K` model search / a Rocky command launcher in a webview |
| `el-dialog` | Confirmations (the AI apply-gate), a settings panel |
| `el-autocomplete` / `el-select` | Model picker / "jump to model"; the color-by and overlay selects |
| `el-dropdown` / `el-popover` | The lineage canvas context menu; info popovers on nodes |
| `el-disclosure` | Collapsible Overview / Columns sections in the Inspector |
| `el-tabs` | The Inspector tab bar (the hand-rolled one works — swap only for the a11y/keyboard win) |

## Gotchas

- **`Cmd+K` collides with VS Code's chord prefix** — a webview `keydown` for `Meta`/`Ctrl`+`K` may be swallowed by the workbench. Give the feature a visible button trigger as the reliable path; treat the shortcut as a bonus.
- **`el-command-palette` empty state**: with no `<el-defaults>`, an empty query renders `<el-no-results>` even while the option list is visible. For a browse-then-filter palette, drop `<el-no-results>` (empty = browse all, typing filters) or supply `<el-defaults>`.

## Licensing

Using the library in this (open-source) extension app is fine — it's an app, not a component library. Do **not** commit TailwindPlus's component markup or docs verbatim into the repo; adapt the patterns and re-theme to `--vscode-*`. That re-theming is required anyway (their palette would clash with the workbench), so it falls out naturally.

See the webview architecture in [`editors/vscode/CLAUDE.md`](../../../CLAUDE.md) and the React apps under `editors/vscode/webview-ui/`.
