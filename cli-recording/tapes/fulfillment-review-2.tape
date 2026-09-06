# fulfillment-review, tape 2 of 3 — the deny and the approve.
#
# The screencast's decision beat. `rocky review` has no --deny verb, and it
# does not need one: a deny is the absence of an approval. A bare apply of the
# pending plan is REFUSED, with its reason, until a marker exists. The same
# command then succeeds once a person has signed off.
#
# Approving stays in the terminal. The browser holds a read-only token and
# physically cannot do it — `rocky serve --ui` refuses to start otherwise.
#
# $PLAN is set by record-screencast.sh from the queue the previous tape just
# printed, so the id on screen is the id the browser was showing.

Output out/fulfillment-review-2.gif

Set Shell bash
Set FontSize 18
Set Width 1200
Set Height 700
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

Hide
Type "cd scratch/fulfillment-review" Enter
Type "export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1" Enter
Type "source .screencast-env" Enter
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 800ms

# 1. THE DENY. No marker, so the engine refuses. Nothing was decided yet, and
#    the engine treats "not yet decided" as "not allowed".
Type 'rocky apply $PLAN' Sleep 400ms Enter
Sleep 6s

# 2. THE APPROVE. A person reads the plan — that is what the browser was for —
#    and signs off here. The marker records a git identity.
Type 'rocky review $PLAN --approve' Sleep 400ms Enter
Sleep 7s

# 3. The same loop command now gets past the gate: it applies the plan and
#    starts observing what it built.
Type "rocky fulfill revenue_daily" Sleep 400ms Enter
Sleep 12s
