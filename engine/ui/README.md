# rocky-ui

The browser UI that `rocky serve --ui` embeds. A React shell over `/api/v1`; every value it shows comes from a typed engine payload, and nothing it loads comes from another host.

```bash
npm ci
npm run build      # writes dist/, then refuses any external load
npm test           # vitest
npm run typecheck
npm run lint
```

`cargo build --features ui` (from `engine/`) embeds `dist/` into the binary. Plain `cargo build` needs no node toolchain. The generated TypeScript types come from `just codegen` and are imported through the `@rocky-types/*` alias, so this package has no copy of them.

Local development: run `rocky serve --token t --token-scope read-only` on port 8080, then `npm run dev` and open the printed Vite address with `#token=t`.
