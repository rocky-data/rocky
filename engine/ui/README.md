# rocky-ui

The browser UI that `rocky serve --ui` embeds. A React shell over `/api/v1`; every value it shows comes from a typed engine payload, and nothing it loads comes from another host. Release binaries and the container image already include it. The [browser UI guide](https://rocky-data.dev/guides/browser-ui/) shows each screen.

![The estate screen: the project strip and the DAG of the playground's three models](../../docs/public/ui-estate.png)

```bash
npm ci
npm run build      # writes dist/, then refuses any external load
npm test           # vitest
npm run typecheck
npm run lint
```

`cargo build --features ui` (from `engine/`) embeds `dist/` into the binary. Plain `cargo build` needs no node toolchain. The generated TypeScript types come from `just codegen` and are imported through the `@rocky-types/*` alias, so this package has no copy of them.

Local development, one command from the repository root:

```bash
just ui-dev                          # the playground's default POC, a transformation pipeline
just ui-dev path/to/your/project
```

It builds this checkout's `rocky`, starts `rocky serve` in that project (default `examples/playground/pocs/00-foundations/00-playground-default`) on port 8080, waits for `/api/v1/health`, then starts Vite on 5173. Open `http://localhost:5173/ui/`. Ctrl-C stops both; if either process dies the other is stopped. `ROCKY_UI_DEV_PORT` overrides the port, and Vite's proxy follows it (`ROCKY_API` in `vite.config.ts`), so the two cannot disagree.

The dev server runs on loopback with no token and without `--ui`, so the page needs no sign-in. `/login` and the session cookie exist only under `--ui`, which needs the page embedded in the binary. With no token, `/api/v1/meta` reports `token_scope: null`, and the page shows its write controls.

By hand: run `rocky serve` on port 8080 (loopback, no token), then `npm run dev` and open `http://localhost:5173/ui/`. To try the sign-in flow, build the page into the binary (`npm run build`, then `cargo build --features ui`) and run `rocky serve --ui`.
