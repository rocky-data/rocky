---
title: Installation
description: How to install Rocky on macOS, Linux, and Windows
sidebar:
  order: 2
---

## Quick Install (macOS / Linux)

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
```

This downloads the latest release binary to `~/.local/bin/rocky`. The script automatically verifies checksums and detects your platform.

To install to a custom directory:

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | ROCKY_INSTALL_DIR=/usr/local/bin bash
```

To install a specific version:

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash -s -- 1.0.0
```

Make sure `~/.local/bin` is in your `PATH`. Add this to your shell profile if needed:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

## Windows

```powershell
irm https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.ps1 | iex
```

## GitHub Releases

Download pre-built binaries from the [releases page](https://github.com/rocky-data/rocky/releases):

| Platform | Architecture | Archive |
|----------|-------------|---------|
| macOS | Apple Silicon (M1+) | `rocky-aarch64-apple-darwin.tar.gz` |
| macOS | Intel | `rocky-x86_64-apple-darwin.tar.gz` |
| Linux | x86_64 | `rocky-x86_64-unknown-linux-gnu.tar.gz` |
| Linux | ARM64 | `rocky-aarch64-unknown-linux-gnu.tar.gz` |
| Windows | x86_64 | `rocky-x86_64-pc-windows-msvc.zip` |

## Build from Source

Requires Rust 1.85+ (edition 2024):

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/engine
cargo build --release
# Binary at: target/release/rocky
```

## Verify Installation

```bash
rocky --version
```
