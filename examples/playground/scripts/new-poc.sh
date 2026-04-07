#!/usr/bin/env bash
# Scaffold a new POC under pocs/<category>/<id-name>/ from the template.
set -euo pipefail

if [ $# -ne 2 ]; then
    cat <<EOF
Usage: $0 <category> <id-name>

Categories:
    00-foundations
    01-quality
    02-performance
    03-ai
    04-governance
    05-orchestration
    06-developer-experience
    07-adapters

Example:
    $0 02-performance 07-late-arriving-data
EOF
    exit 1
fi

category=$1
poc_id=$2

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE="$REPO_ROOT/scripts/_poc-template"
TARGET="$REPO_ROOT/pocs/$category/$poc_id"

if [ ! -d "$TEMPLATE" ]; then
    echo "Template not found at $TEMPLATE"
    exit 1
fi

if [ -d "$TARGET" ]; then
    echo "POC directory already exists: $TARGET"
    exit 1
fi

if [ ! -d "$REPO_ROOT/pocs/$category" ]; then
    echo "Unknown category '$category'. Run $0 with no args to see valid categories."
    exit 1
fi

cp -r "$TEMPLATE" "$TARGET"
chmod +x "$TARGET/run.sh"

echo "Created POC at: $TARGET"
echo "Next steps:"
echo "  1. Edit $TARGET/README.md"
echo "  2. Edit $TARGET/rocky.toml + models/ + contracts/ + seeds/"
echo "  3. Test: cd $TARGET && ./run.sh"
echo "  4. Add a row to pocs/README.md and the top-level README.md catalog"
