---
title: Changelog
description: Where to find release notes for each Rocky artifact.
sidebar:
  order: 8
---

Rocky ships four artifacts, each on its own version number, so each keeps its own changelog. The notes themselves live in the repository and on the release pages below. This page points you at them.

## The four artifacts

| Artifact | Version tag | Changelog | Release page |
|---|---|---|---|
| `rocky` CLI (engine) | `engine-v*` | [engine/CHANGELOG.md](https://github.com/rocky-data/rocky/blob/main/engine/CHANGELOG.md) | [GitHub Releases](https://github.com/rocky-data/rocky/releases) |
| `rocky-sdk` wheel | `sdk-v*` | [sdk/python/CHANGELOG.md](https://github.com/rocky-data/rocky/blob/main/sdk/python/CHANGELOG.md) | [PyPI](https://pypi.org/project/rocky-sdk/#history) |
| `dagster-rocky` wheel | `dagster-v*` | [integrations/dagster/CHANGELOG.md](https://github.com/rocky-data/rocky/blob/main/integrations/dagster/CHANGELOG.md) | [PyPI](https://pypi.org/project/dagster-rocky/#history) |
| Rocky VS Code extension | `vscode-v*` | [editors/vscode/CHANGELOG.md](https://github.com/rocky-data/rocky/blob/main/editors/vscode/CHANGELOG.md) | [Marketplace](https://marketplace.visualstudio.com/items?itemName=rocky-data.rocky) |

Every changelog follows [Keep a Changelog](https://keepachangelog.com/), and the project uses [semantic versioning](https://semver.org/).

## Why four version numbers

The four artifacts move at different speeds and couple to each other loosely, so each ships on its own schedule. The SDK and the extension both call the `rocky` binary as a subprocess, and the Dagster integration is a thin adapter over the SDK. A CLI release therefore forces no release of the others.

Tagging `engine-v1.47.0` builds and publishes the CLI alone. The SDK, Dagster, and extension releases work the same way under their own tags.

```
   engine-v*  ──►  rocky CLI binary        (GitHub Releases)
   sdk-v*     ──►  rocky-sdk wheel         (PyPI)
   dagster-v* ──►  dagster-rocky wheel     (PyPI)
   vscode-v*  ──►  Rocky VSIX              (Marketplace)
```

## Upgrading

Upgrade the CLI to the newest release:

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
```

Pin a specific version by passing it to the installer:

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash -s -- <version>
```
