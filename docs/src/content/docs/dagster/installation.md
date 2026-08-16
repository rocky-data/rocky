---
title: Installation
description: How to install dagster-rocky
sidebar:
  order: 2
---

`dagster-rocky` is the Python package that lets Dagster run Rocky. Install it
into the same environment as your Dagster code location, then install the
`rocky` binary it calls. A code location is the Python process where Dagster
loads your definitions.

You install two things because Rocky itself is a Rust program. The Python
package only builds the commands and reads the JSON that comes back.

```
┌──────────────────┐  Python call  ┌────────────────────┐
│ your Dagster     │──────────────►│ dagster-rocky      │
│ code location    │               │ (+ rocky-sdk)      │
└──────────────────┘               └─────────┬──────────┘
                                             │ subprocess
                                             ▼
                                   ┌────────────────────┐   SQL
                                   │ rocky binary       │───────► warehouse
                                   │ (Rust, installed   │
                                   │  separately)       │
                                   └────────────────────┘
```

## Install the package

```bash
uv add dagster-rocky
```

## Dependencies

The package requires:

- `rocky-sdk >= 0.6.0` (the typed result models and `RockyClient` live here; `dagster-rocky` is a thin adapter over it)
- `dagster >= 1.13.8`
- `pydantic >= 2.0`
- `pygments >= 2.20.0`

## Install the Rocky binary

Install the `rocky` binary separately. The Python package does not bundle it.
The [Installation](/getting-started/installation/) page has the steps for each
platform.

Put the binary on your `PATH`. If you would rather keep it somewhere else, set
the `binary_path` config on `RockyResource` to its location.

### Vendor binary for deployment

Vendor the binary next to your Dagster code when you want to pin one Rocky
version. This suits containers and cloud deployments, where the host machine
has nothing installed.

```
my_dagster_project/
  dagster_project/
    __init__.py
    definitions.py
  vendor/
    rocky          # platform-specific binary
  rocky.toml
```

Then point the resource at the vendored path:

```python
rocky = RockyResource(
    binary_path="vendor/rocky",
    config_path="rocky.toml",
)
```

Download the binary for your target platform from [GitHub Releases](https://github.com/rocky-data/rocky/releases). Filter by the `engine-v*` tag prefix. The `engine/install.sh` script can do this for you:

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | ROCKY_INSTALL_DIR=vendor bash
```

## Verify the installation

```bash
python -c "from dagster_rocky import RockyResource; print('ok')"
```

The package is installed correctly if this prints `ok`. Check the binary
separately:

```bash
rocky --version
```
