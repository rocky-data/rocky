# fulfillment-review, tape 1 of 3 — a sentence becomes a data product.
#
# Cold start through the loop's first full round: the recorded elicitation
# writes a candidate spec, a human approves it, and one `rocky fulfill` drafts,
# verifies RED (E010 — the draft forgot revenue_eur), repairs, verifies green
# and proposes a plan. The plan then waits for a person.
#
# Driven by record-screencast.sh, NOT by record.sh: all three tapes share one
# prepared workspace, because the plan id on screen here has to be the same one
# the browser shows next.

Output out/fulfillment-review-1.gif

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
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 800ms

# 1. Nothing exists yet. The brief is a paragraph a person wrote.
Type "cat briefs/elicitation.md" Sleep 400ms Enter
Sleep 3s

# 2. The loop reads it and writes a candidate spec. It stops there, on purpose:
#    a spec is a promise about data, so a human approves it.
Type "rocky fulfill revenue_daily" Sleep 400ms Enter
Sleep 6s

Type "cat products/revenue_daily.toml" Sleep 400ms Enter
Sleep 5s

# 3. The human gate. This is a person's decision, in a terminal.
Type "rocky fulfill approve-spec revenue_daily" Sleep 400ms Enter
Sleep 4s

# 4. One command drives the rest: draft, verify RED, repair, verify green,
#    propose. The repair round is the interesting part — the first draft
#    forgot a required column and the engine caught it, not a reviewer.
Type "rocky fulfill revenue_daily" Sleep 400ms Enter
Sleep 12s

# 5. The plan is waiting for a person, and the queue says why.
Type "rocky review --queue" Sleep 400ms Enter
Sleep 6s
