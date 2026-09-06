# fulfillment-review, tape 3 of 3 — what the engine keeps watching.
#
# The product is built. The loop does not stop there: it observes the data it
# produced, against the checks the spec declared. This tape shows the one
# observation a build alone can never make — the output going stale — and
# leaves the full custody chain to the browser, which renders all 82 rows of
# it rather than the last few a `tail` would fit.
#
# Every visible command uses the default table output. A pipe would make
# stdout a non-tty, and `rocky` answers a non-tty in JSON.

Output out/fulfillment-review-3.gif

Set Shell bash
Set FontSize 18
Set Width 1200
Set Height 700
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

Hide
Type "cd scratch/fulfillment-review" Enter
# ROCKY_SCRUB_HOST keeps the recording machine's hostname out of the
# approval marker, and out of the frame.
Type "export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1 ROCKY_SCRUB_HOST=1" Enter
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 800ms

# 1. Where the product stands: its spec, its approval, and the loop's state.
Type "rocky product status revenue_daily" Sleep 400ms Enter
Sleep 7s

# 2. Staleness is OBSERVED, not enforced. Backdate what was materialised and
#    the loop reports the lag against the spec's budget — it does not fail the
#    product, because "too old" is a judgment its owner makes, not the engine.
Type "cat backdate.sql" Sleep 400ms Enter
Sleep 3s
Type "duckdb wh.duckdb < backdate.sql" Sleep 400ms Enter
Sleep 2s
Type "rocky fulfill revenue_daily" Sleep 400ms Enter
Sleep 10s

# 3. And the loop has been writing all of it down. Every transition above is a
#    persisted journal row; the browser reads the whole chain.
Type "rocky product status revenue_daily" Sleep 400ms Enter
Sleep 6s
