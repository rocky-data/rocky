#!/usr/bin/env bash
# record.sh <demo> — prepare a clean scratch workspace then render its tape.
#
#   ./record.sh quickstart       -> out/quickstart.gif
#   ./record.sh column-lineage   -> out/column-lineage.gif
#
# Requires `vhs` (brew install vhs) and `rocky` on $PATH.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

demo="${1:?usage: record.sh <demo>   (e.g. quickstart, column-lineage)}"
tape="tapes/$demo.tape"

if [[ ! -f "$tape" ]]; then
    echo "record.sh: no tape at $tape" >&2
    exit 1
fi
command -v vhs >/dev/null || { echo "record.sh: vhs not found — brew install vhs" >&2; exit 1; }
command -v rocky >/dev/null || { echo "record.sh: rocky not found on \$PATH" >&2; exit 1; }

./prepare.sh "$demo"

mkdir -p out
# vhs resolves the tape's `Output` path relative to its own cwd, so run from
# here and let the tape write to out/<demo>.gif.
vhs "$tape"

echo "recorded out/$demo.gif"
