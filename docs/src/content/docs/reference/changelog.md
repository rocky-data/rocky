---
title: Changelog
description: Where to find release notes for each Rocky artifact.
sidebar:
  order: 8
---

Rocky ships four artifacts on independent version numbers, so each keeps its own changelog. The canonical notes live in the repository and on the release pages below; this page just points you to them.

## The four artifacts

| Artifact | Version tag | Changelog | Release page |
|---|---|---|---|
| `rocky` CLI (engine) | `engine-v*` | [engine/CHANGELOG.md](https://github.com/rocky-data/rocky/blob/main/engine/CHANGELOG.md) | [GitHub Releases](https://github.com/rocky-data/rocky/releases) |
| `rocky-sdk` wheel | `sdk-v*` | [sdk/python/CHANGELOG.md](https://github.com/rocky-data/rocky/blob/main/sdk/python/CHANGELOG.md) | [PyPI](https://pypi.org/project/rocky-sdk/#history) |
| `dagster-rocky` wheel | `dagster-v*` | [integrations/dagster/CHANGELOG.md](https://github.com/rocky-data/rocky/blob/main/integrations/dagster/CHANGELOG.md) | [PyPI](https://pypi.org/project/dagster-rocky/#history) |
| Rocky VS Code extension | `vscode-v*` | [editors/vscode/CHANGELOG.md](https://github.com/rocky-data/rocky/blob/main/editors/vscode/CHANGELOG.md) | [Marketplace](https://marketplace.visualstudio.com/items?itemName=rocky-data.rocky) |

Each changelog follows [Keep a Changelog](https://keepachangelog.com/) and the project uses [semantic versioning](https://semver.org/).

## Why four version numbers

The CLI, the Python SDK, the Dagster integration, and the VS Code extension are released separately because they move at different speeds and depend on each other loosely. The SDK and the extension both call the `rocky` binary as a subprocess, and the Dagster integration is a thin adapter over the SDK, so a CLI release doesn't force a release of the others. Tagging `engine-v1.47.0` builds and publishes only the CLI; the SDK, Dagster, and extension releases work the same way under their own tags.

## Upgrading

To upgrade the CLI to the latest release:

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
```

Pin a specific version by passing it to the installer:

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash -s -- <version>
```
