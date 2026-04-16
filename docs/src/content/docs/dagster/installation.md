---
title: Installation
description: How to install dagster-rocky
sidebar:
  order: 2
---

## Install the package

```bash
uv add dagster-rocky
```

## Dependencies

The package requires:

- `dagster >= 1.13.0`
- `pydantic >= 2.0`
- `pygments >= 2.20.0`

These are installed automatically as package dependencies.

## Install the Rocky binary

The `rocky` binary must be installed separately. It is not bundled with the Python package. See the [Installation](/getting-started/installation/) page for instructions on installing Rocky for your platform.

The binary must be available on your `PATH`, or you can specify its location explicitly via the `binary_path` config on `RockyResource`.

### Vendor binary for deployment

For containerized or cloud deployments where you want to pin a specific Rocky version, you can vendor the binary alongside your Dagster code:

```
my_dagster_project/
  dagster_project/
    __init__.py
    definitions.py
  vendor/
    rocky          # platform-specific binary
  rocky.toml
```

Then configure the resource to use the vendored path:

```python
rocky = RockyResource(
    binary_path="vendor/rocky",
    config_path="rocky.toml",
)
```

Download the binary for your target platform from [GitHub Releases](https://github.com/rocky-data/rocky/releases) (filter by the `engine-v*` tag prefix). The `engine/install.sh` script can automate this:

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | ROCKY_INSTALL_DIR=vendor bash
```

## Verify the installation

```bash
python -c "from dagster_rocky import RockyResource; print('ok')"
```

If this prints `ok`, the package is installed correctly. You can also verify the binary is accessible:

```bash
rocky --version
```
