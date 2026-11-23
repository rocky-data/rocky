# VS Code release

Part of the `rocky-release` skill. Read `../SKILL.md` first.

## VS Code release (default: just tag and push)

```bash
# 1. Bump package.json + CHANGELOG in a PR, merge to main.
# 2. Tag the merged commit and push:
git tag -a vscode-v0.3.0 -m "Release vscode-v0.3.0"
git push origin vscode-v0.3.0
```

The tag push triggers `vscode-release.yml`, which:

1. `ensure-release` — creates the `vscode-v0.3.0` GitHub Release if missing, as a **draft** (`--draft --latest=false`). An existing draft (the local fallback) is used as it is. An existing published release stops the run.
2. `build` — `npx vsce package` produces the VSIX and attaches it with `gh release upload`; `vsce publish` pushes to the VS Code Marketplace using the `VSCE_PAT` repo secret; then the release is published (`gh release edit --draft=false --latest=false`) as the last step. In `rocky-data/rocky` an unset `VSCE_PAT` fails the run and the release stays a draft. A fork without the secret skips the Marketplace step.

### VS Code fallback: local build

```bash
just release-vscode 0.3.0                 # GH release only
just release-vscode 0.3.0 --publish       # + Marketplace via local VSCE_PAT
```
