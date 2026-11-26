# CI surface and post-release checklist

Part of the `rocky-release` skill. Read `../SKILL.md` first.

## CI surface

Path-filtered workflows in `.github/workflows/`:

- `engine-ci.yml` — test + clippy + fmt on every PR touching `engine/**`
- `engine-weekly.yml` — coverage (tarpaulin) + cargo-audit, Monday schedule + manual dispatch
- `engine-bench.yml` — only PRs labeled `perf` touching `engine/crates/**` or `engine/Cargo.*`
- `engine-release.yml` — **full 5-target matrix build on tag `engine-v*` push**. Owns the GitHub Release creation + binary uploads + `checksums.txt`.
- `sdk-release.yml` / `dagster-release.yml` / `vscode-release.yml` — tag-triggered (`sdk-v*` / `dagster-v*` / `vscode-v*`) release + publish
- `engine-wasm-release.yml` — builds `rocky-wasm` and publishes to npm on `engine-wasm-v*` tags (independent of CLI releases). Same draft-until-last-step shape as the other four; an unset `NPM_TOKEN` fails the run here and stays a no-op on a fork
- `engine-docs.yml` — build + deploy Astro docs from `docs/` to GitHub Pages
- `codegen-drift.yml` — fails any PR where committed bindings drift from `just codegen` output

## Post-release checklist

- [ ] `gh release view <tag>` shows all expected artifacts (**11 for engine**: 10 archives + `checksums.txt`; **4 for sdk and 4 for dagster**: wheel, sdist and one `.publish.attestation` for each, uploaded by the PyPI trusted-publisher step; 1 for vscode) and `gh release view <tag> --json isDraft` is `false`
- [ ] Install script (`engine/install.sh` or `install.ps1`) resolves and installs the new version on a clean machine
- [ ] Browser UI screenshots match the release. With the new engine on `PATH`, run `./cli-recording/record-ui-screenshots.sh --publish` and look at each image before committing. Skip it when the release changed nothing the UI shows. The images in `docs/public/` must show a binary a reader can install, so they are recaptured from the tagged build, never from `main`.
- [ ] No docs page still announces this release's UI change as coming. `docs/src/content/docs/guides/browser-ui.md` carries one such line under the tour GIF: it tells a reader the screenshots show the current release and names what arrives in the next one. Once the tag ships that change, the line is false. Delete it, or point it at whatever is now next.
- [ ] Changelog is on `main` (it merged with the release PR, but double-check)
- [ ] Announcement, if public-facing (blog, release notes)
