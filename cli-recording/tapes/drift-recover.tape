# drift-recover — replicate, drift the source type, let Rocky DROP + RECREATE.
# Matches README alt-text: "schema drift detected and the target recreated".
# Render with:  ./record.sh drift-recover   (runs prepare.sh first)

Output out/drift-recover.gif

Set Shell bash
Set FontSize 18
Set Width 1200
Set Height 700
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

# Quiet setup: enter the pre-seeded scratch dir, suppress engine info logs and
# the run-deprecation banner, minimal "> " prompt. None of this shows.
Hide
Type "cd scratch/drift-recover" Enter
Type "export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1" Enter
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 600ms

# 1. First run — copy raw__orders.orders into the staging schema.
Type "rocky -c rocky.toml run --filter source=orders --output table" Sleep 400ms Enter
Sleep 2500ms

# 2. The source schema changes underneath us: amount DECIMAL -> VARCHAR.
Type 'duckdb poc.duckdb "ALTER TABLE raw__orders.orders ALTER COLUMN amount TYPE VARCHAR"' Sleep 400ms Enter
Sleep 1800ms

# 3. Second run — Rocky detects the drift and recreates the target.
Type "rocky -c rocky.toml run --filter source=orders --output table" Sleep 400ms Enter
Sleep 3000ms

Sleep 2s
