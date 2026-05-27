---
name: tailwind-plus-elements
description: Using TailwindPlus Elements (@tailwindplus/elements) in the Rocky VS Code webviews. Use when building or upgrading an interactive webview component — a command palette / model search, a confirmation dialog, an autocomplete or select model picker, a dropdown/popover menu, a collapsible disclosure, or tabs. Covers the esbuild-bundle install (no CSP change vs the CDN), the --vscode-* theming + Tailwind v3.4 data-variant nuance, React 19 custom-element interop (boolean attrs, native events via ref, the elements:ready gate), and which element fits which Rocky surface.
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

Transitions use read-only `data-closed` / `data-enter` / `data-leave` / `data-transition` attributes. We are on **Tailwind v3.4**, so target them with the **arbitrary-variant** form — `data-[closed]:opacity-0`, `transition data-[closed]:scale-95` — **not** the v4 `data-closed:` shorthand the official docs use.

## React 19 interop

- Render `<el-dialog>` etc. directly in JSX. Add ambient typings for the `el-*` tags you use (a `declare global { namespace JSX { interface IntrinsicElements { "el-dialog": …; } } }` in a `.d.ts` under `webview-ui/`), or cast.
- **State** — prefer the declarative attributes: `open` on `<el-dialog>`, `hidden` on `<el-disclosure>`, driven from React state. For booleans on custom elements, render the attribute conditionally (`{...(isOpen ? { open: "" } : {})}`) since React sets primitives as attributes on unknown tags.
- **Open/close** can also be fully declarative via the Invoker Commands API — `<button command="show-modal" commandfor="id">` / `command="close"`, and the disclosure's `command="--toggle"` / `--show` / `--hide`. No React wiring needed for these.
- **Events** (`change`, `open`, `close`, `cancel`, …) are native `CustomEvent`s, not React synthetic events — wire them with a `ref` + `addEventListener` in a `useEffect`, not an `onChange` prop. `cancel` is `preventDefault()`-able.
- **Imperative** — instances expose `show()` / `hide()` / `toggle()` / `reset()` / `setFilterCallback(cb)`. If you must touch one in JS before it's defined, gate on readiness: `if (customElements.get("el-dialog")) … else window.addEventListener("elements:ready", …)` (rarely needed since we import at module load).

## Which element fits which Rocky surface

| Element | Rocky use |
|---|---|
| `el-command-palette` (inside `el-dialog`) | `Cmd+K` model search / a Rocky command launcher in a webview |
| `el-dialog` | Confirmations (the AI apply-gate), a settings panel |
| `el-autocomplete` / `el-select` | Model picker / "jump to model"; the color-by and overlay selects |
| `el-dropdown` / `el-popover` | The lineage canvas context menu; info popovers on nodes |
| `el-disclosure` | Collapsible Overview / Columns sections in the Inspector |
| `el-tabs` | The Inspector tab bar (the hand-rolled one works — swap only for the a11y/keyboard win) |

## Licensing

Using the library in this (open-source) extension app is fine — it's an app, not a component library. Do **not** commit TailwindPlus's component markup or docs verbatim into the repo; adapt the patterns and re-theme to `--vscode-*`. That re-theming is required anyway (their palette would clash with the workbench), so it falls out naturally.

See the webview architecture in [`editors/vscode/CLAUDE.md`](../../../CLAUDE.md) and the React apps under `editors/vscode/webview-ui/`.
