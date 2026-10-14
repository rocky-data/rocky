#!/usr/bin/env bash
# Build the Linux release binaries (`rocky` and `rocky-lsp`) with gcc, inside
# a manylinux_2_28 container.
#
# engine-release.yml runs this with `docker run` in a digest-pinned
# quay.io/pypa/manylinux_2_28_<arch> image, on a runner of the same
# architecture. The runner's Rust toolchain is mounted in (CARGO_HOME and
# RUSTUP_HOME), so this script downloads no toolchain of its own.
#
# Why gcc and not cargo-zigbuild: zig builds the bundled DuckDB C++ against
# LLVM's libc++. The DuckDB extensions that DuckDB downloads at run time
# (icu, httpfs, ...) are built against GNU libstdc++. Loading one into a
# zigbuild binary crashed engine 1.81.0 with a segfault (#2367).
# manylinux_2_28 keeps the glibc floor at 2.28, the floor the zigbuild
# release had.
#
# Usage: scripts/linux_release_build.sh <target>
#   e.g. scripts/linux_release_build.sh x86_64-unknown-linux-gnu

set -euo pipefail

target="${1:?usage: linux_release_build.sh <target>}"

export PATH="${CARGO_HOME:?CARGO_HOME must point at the mounted cargo home}/bin:$PATH"
: "${RUSTUP_HOME:?RUSTUP_HOME must point at the mounted rustup home}"

cd "$(dirname "${BASH_SOURCE[0]}")/../engine"

gcc --version | head -1
rustc --version

cargo build --release --bin rocky --bin rocky-lsp --features ui --target "$target"
