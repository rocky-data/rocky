---
title: Installation
description: Install the Rocky CLI on macOS, Linux, or Windows
sidebar:
  order: 2
---

## macOS / Linux

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
```

Installs to `~/.local/bin/rocky` and verifies checksums. Make sure `~/.local/bin` is on your `PATH`:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

**Custom install directory:**

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh \
  | ROCKY_INSTALL_DIR=/usr/local/bin bash
```

**Pin a specific version:**

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash -s -- 1.0.0
```

## Windows

```powershell
irm https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.ps1 | iex
```

## Pre-built binaries

Direct downloads from the [releases page](https://github.com/rocky-data/rocky/releases):

| Platform | Archive |
|---|---|
| macOS (Apple Silicon) | `rocky-aarch64-apple-darwin.tar.gz` |
| macOS (Intel) | `rocky-x86_64-apple-darwin.tar.gz` |
| Linux x86_64 | `rocky-x86_64-unknown-linux-gnu.tar.gz` |
| Linux ARM64 | `rocky-aarch64-unknown-linux-gnu.tar.gz` |
| Windows x86_64 | `rocky-x86_64-pc-windows-msvc.zip` |

## Build from source

Requires Rust 1.85+ (edition 2024).

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/engine
cargo build --release
# binary at target/release/rocky
```

## Verify

```bash
rocky --version
```
