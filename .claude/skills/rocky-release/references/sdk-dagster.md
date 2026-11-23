# SDK and Dagster release

Part of the `rocky-release` skill. Read `../SKILL.md` first.

## SDK release (default: just tag and push)

```bash
# 1. Bump sdk/python/pyproject.toml + sdk/python/CHANGELOG.md in a PR, merge to main.
# 2. Tag the merged commit and push:
git tag -a sdk-v0.2.0 -m "Release sdk-v0.2.0"
git push origin sdk-v0.2.0
```

The tag push triggers `sdk-release.yml`, which:

1. `ensure-release` — creates the `sdk-v0.2.0` GitHub Release if missing, as a **draft** (`--draft --latest=false`). An existing draft (the local fallback) is used as it is. An existing published release stops the run.
2. `publish-pypi` — `uv build`, publish via `pypa/gh-action-pypi-publish` using **OIDC** (trusted publisher; no token in repo secrets), attach `dist/*` to the GH Release (wheel, sdist, and the two `.publish.attestation` files the publish step writes), then publish the release (`gh release edit --draft=false --latest=false`) as the last step.

**Ordering rule:** release `rocky-sdk` *before* any `dagster-rocky` release that raises its `rocky-sdk>=…` floor — the published dagster wheel resolves the SDK from PyPI, not the monorepo path source.

### SDK fallback: local build

```bash
just release-sdk 0.2.0                # GH release only
just release-sdk 0.2.0 --publish      # + PyPI
```

## Dagster release (default: just tag and push)

```bash
# 1. Bump pyproject.toml + CHANGELOG in a PR, merge to main.
# 2. Tag the merged commit and push:
git tag -a dagster-v0.4.0 -m "Release dagster-v0.4.0"
git push origin dagster-v0.4.0
```

The tag push triggers `dagster-release.yml`, which:

1. `ensure-release` — creates the `dagster-v0.4.0` GitHub Release if missing, as a **draft** (`--draft --latest=false`). An existing draft (the local fallback) is used as it is. An existing published release stops the run.
2. `publish-pypi` — `uv build`, publish via `pypa/gh-action-pypi-publish` using **OIDC** (trusted publisher; no token in repo secrets), attach `dist/*` to the GH Release (wheel, sdist, and the two `.publish.attestation` files), then publish the release (`gh release edit --draft=false --latest=false`) as the last step.

### Dagster fallback: local build

```bash
just release-dagster 0.4.0                # GH release only
just release-dagster 0.4.0 --publish      # + PyPI via UV_PUBLISH_TOKEN or ~/.pypirc
```

Without `--publish`, the local path creates a draft and the tag push's CI run publishes it after the PyPI upload. With `--publish`, PyPI already has the wheel, so the script creates the draft, attaches the artifacts, and then publishes it in a separate step; the tag push's CI run then stops at `ensure-release` (already published). That red run is expected. Only reach for this when the CI workflow itself is broken.
