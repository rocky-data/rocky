#!/usr/bin/env bash
# Smoke-test a Linux release binary before engine-release.yml publishes it.
#
# Checks, in order:
#   1. glibc floor: the binary needs no glibc symbol newer than 2.28.
#   2. Every shared library the binary needs resolves.
#   3. A real DuckDB extension downloads and loads. A playground model that
#      needs `icu` (`current_date + 1`, a time-zone conversion) must run.
#      Engine 1.81.0 crashed on Linux exactly here (#2367), and no release
#      step ran the Linux binary before it was published.
#
# Needs network access: DuckDB downloads the icu extension.
#
# Usage: scripts/linux_release_smoke.sh <path-to-rocky-binary>

set -euo pipefail

rocky="$(realpath "${1:?usage: linux_release_smoke.sh <path-to-rocky-binary>}")"

max_glibc="$(objdump -T "$rocky" | grep -o 'GLIBC_[0-9][0-9.]*' | sed 's/^GLIBC_//' | sort -uV | tail -1)"
echo "Highest glibc symbol version: $max_glibc"
if [ "$(printf '%s\n' "$max_glibc" 2.28 | sort -V | tail -1)" != "2.28" ]; then
  echo "::error::$rocky needs glibc $max_glibc; the release floor is 2.28"
  exit 1
fi

if ldd "$rocky" | grep -q "not found"; then
  ldd "$rocky"
  echo "::error::$rocky needs a shared library that is not installed"
  exit 1
fi

"$rocky" --version

work="$(mktemp -d)"
"$rocky" playground "$work/pg" --template quickstart > /dev/null
cd "$work/pg"
cp models/raw_orders.toml models/icu_probe.toml
printf "SELECT current_date + 1 AS tomorrow, now() AT TIME ZONE 'Europe/Lisbon' AS lisbon_now\n" \
  > models/icu_probe.sql

status=0
"$rocky" run > run.json 2> run.err || status=$?
if [ "$status" -ne 0 ]; then
  tail -50 run.err
  echo "::error::rocky run exited with status $status"
  exit 1
fi
if grep -q "could not load the DuckDB icu extension" run.err; then
  grep "icu" run.err
  echo "::error::the DuckDB icu extension did not load"
  exit 1
fi
if ! grep -q '"status": *"Success"' run.json || ! grep -q '"tables_failed": *0' run.json \
  || ! grep -q '"icu_probe"' run.json; then
  cat run.json
  echo "::error::rocky run did not materialize every model, icu_probe included"
  exit 1
fi

echo "Smoke passed: glibc <= 2.28, libraries resolve, the icu extension loads"
