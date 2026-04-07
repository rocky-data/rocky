---
title: Installation
description: How to install dagster-rocky
sidebar:
  order: 2
---

## Install the package

```bash
pip install dagster-rocky
```

Or with `uv`:

```bash
uv add dagster-rocky
```

## Dependencies

The package requires:

- `dagster >= 1.12.0`
- `pydantic >= 2.0`

These are installed automatically as package dependencies.

## Install the Rocky binary

The `rocky` binary must be installed separately. It is not bundled with the Python package. See the [Installation](/getting-started/installation/) page for instructions on installing Rocky for your platform.

The binary must be available on your `PATH`, or you can specify its location explicitly via the `binary_path` config on `RockyResource`.

## Verify the installation

```bash
python -c "from dagster_rocky import RockyResource; print('ok')"
```

If this prints `ok`, the package is installed correctly. You can also verify the binary is accessible:

```bash
rocky --version
```
