# incremental-watermark — run, append a delta, run again; the watermark advances
# so only the new rows move. Matches README alt-text: "incremental copies only
# the new rows; the watermark advances".
# Render with:  ./record.sh incremental-watermark   (runs prepare.sh first)
#
# HELD — published GIF stays hand-made. On rocky 1.44.0 the persisted watermark
# is the run's wall-clock time, not max(occurred_at) from the copied rows, so
# run 2 filters with `WHERE occurred_at > <wall-clock-now>` and copies zero
# delta rows (target stays at 500, never 525). The reproduction therefore shows
# the watermark moving *backwards* (wall-clock now -> the data's max occurred_at)
# instead of advancing. The tape is kept so a fix to the POC seeds / engine
# watermark behaviour can be re-rendered and republished; until then
# docs/public/demo-incremental-watermark.gif keeps its hand-made original.

Output out/incremental-watermark.gif

Set Shell bash
Set FontSize 18
Set Width 1200
Set Height 700
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

Hide
Type "cd scratch/incremental-watermark" Enter
Type "export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1" Enter
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 600ms

# 1. First run — copy the initial events into staging.
Type "rocky -c rocky.toml run --filter source=events --output table" Sleep 400ms Enter
Sleep 2200ms

# 2. The watermark Rocky persisted from run 1.
Type "rocky state --output table" Sleep 400ms Enter
Sleep 2000ms

# 3. New events arrive with later timestamps.
Type "duckdb poc.duckdb < data/seed_delta.sql" Sleep 400ms Enter
Sleep 1500ms

# 4. Second run — incremental: only the rows past the watermark are copied.
Type "rocky -c rocky.toml run --filter source=events --output table" Sleep 400ms Enter
Sleep 2200ms

# 5. The watermark has advanced to the newest event timestamp.
Type "rocky state --output table" Sleep 400ms Enter
Sleep 2800ms

Sleep 2s
