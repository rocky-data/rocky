#!/usr/bin/env bash
# intercut.sh — cut the six takes into one screencast.
#
#   ./intercut.sh [take-dir]        # default: out/screencast
#
# record-screencast.sh calls this at the end; it is a separate script so an
# edit can be re-cut from takes that are already on disk, which is most of what
# editing is.
#
# The output is H.264 mp4, not a GIF. The CLI demos next door are GIFs because
# they are ten to fifteen seconds; this is minutes, and a GIF of it would be
# tens of megabytes for a worse picture. `out/` is gitignored — the cut is a
# build output, and where it is published is an editorial decision, not this
# script's.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TAKES="${1:-$HERE/out/screencast}"
OUT="$TAKES/fulfillment-screencast.mp4"
WORK="$TAKES/.parts"

command -v ffmpeg >/dev/null || { echo "intercut.sh: ffmpeg not found" >&2; exit 1; }

# The order is the story: a sentence becomes a spec, the plan is read, it is
# refused and then approved, the rows it built are sampled, it goes stale, and
# the whole custody chain is read back.
PARTS=(
    "terminal-1.gif"
    "review.webm"
    "terminal-2.gif"
    "samples.webm"
    "terminal-3.gif"
    "journal.webm"
)

missing=0
for p in "${PARTS[@]}"; do
    [ -s "$TAKES/$p" ] || { echo "intercut.sh: missing take $TAKES/$p" >&2; missing=1; }
done
[ "$missing" = "0" ] || { echo "run ./record-screencast.sh first" >&2; exit 1; }

rm -rf "$WORK"; mkdir -p "$WORK"

# Normalise every take to one codec, size and frame rate before concatenating.
# The two sources genuinely differ — vhs writes GIFs at its own frame rate,
# Playwright writes VP8 webm — and the concat demuxer will happily produce a
# broken file from mismatched streams rather than refuse.
#
# `scale=…:force_original_aspect_ratio=decrease` then `pad` keeps a take that
# is not exactly 1200x700 (a different viewport, a re-recorded tape) centred
# and undistorted instead of stretched.
i=0
for p in "${PARTS[@]}"; do
    i=$((i + 1))
    part="$WORK/$(printf '%02d' "$i").mp4"
    echo "▶ normalising $p"
    ffmpeg -y -loglevel error -i "$TAKES/$p" \
        -vf "scale=1200:700:force_original_aspect_ratio=decrease,pad=1200:700:(ow-iw)/2:(oh-ih)/2:color=black,fps=30,format=yuv420p" \
        -c:v libx264 -crf 20 -preset medium -an "$part" \
        || { echo "intercut.sh: could not normalise $p" >&2; exit 1; }
    printf "file '%s'\n" "$part" >> "$WORK/parts.txt"
done

echo "▶ concatenating"
# Stream copy: every part is already the same codec, so this is a remux, not a
# second encode. Re-encoding here would cost quality for nothing.
ffmpeg -y -loglevel error -f concat -safe 0 -i "$WORK/parts.txt" -c copy "$OUT" \
    || { echo "intercut.sh: concat failed" >&2; exit 1; }

rm -rf "$WORK"

DUR=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$OUT" 2>/dev/null | cut -d. -f1)
SIZE=$(du -h "$OUT" | cut -f1)
echo
echo "✓ $OUT  (${DUR:-?}s, $SIZE)"
echo
echo "Watch it before doing anything with it. The acceptance rows for U3-A3"
echo "are checked by looking — there is no test that passes here."
