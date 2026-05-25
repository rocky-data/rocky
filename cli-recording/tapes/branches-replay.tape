# branches-replay — run on main, isolate a run on a named branch, then replay.
# Matches README alt-text: "branch isolates a run; replay shows it deterministically".
# Render with:  ./record.sh branches-replay   (runs prepare.sh first)

Output out/branches-replay.gif

Set Shell bash
Set FontSize 18
Set Width 1200
Set Height 700
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

Hide
Type "cd scratch/branches-replay" Enter
Type "export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1" Enter
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 600ms

# 1. Run replication on main — writes poc.staging__orders.orders.
Type "rocky -c rocky.toml run --filter source=orders --output table" Sleep 400ms Enter
Sleep 2200ms

# 2. Create an isolated branch (state-store entry, no warehouse side effects).
Type 'rocky branch create fix_revenue --description "Revised revenue definition" --output table' Sleep 400ms Enter
Sleep 1800ms

# 3. Run against the branch — writes a separate poc.branch__fix_revenue schema.
Type "rocky -c rocky.toml run --filter source=orders --branch fix_revenue --output table" Sleep 400ms Enter
Sleep 2200ms

# 4. Replay the last run — deterministic record: status, hashes, per-model cost.
Type "rocky replay latest --output table" Sleep 400ms Enter
Sleep 3000ms

Sleep 2s
