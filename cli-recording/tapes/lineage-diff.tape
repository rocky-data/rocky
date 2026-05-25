# lineage-diff — PR-time blast radius: compare two git refs, list column changes
# + downstream consumers. Matches README alt-text: "rocky lineage-diff names
# every changed column across two models in one shot".
# Render with:  ./record.sh lineage-diff   (runs prepare.sh first; builds a git repo)

Output out/lineage-diff.gif

Set Shell bash
Set FontSize 18
Set Width 1200
Set Height 700
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

Hide
Type "cd scratch/lineage-diff" Enter
Type "export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1" Enter
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 600ms

# We're on feature/revenue-rework, which renamed amount->amount_usd and added
# two derived columns upstream. Diff against main to see the full blast radius
# across both changed models — the comment that lands on a PR.
Type "rocky lineage-diff main --output table" Sleep 400ms Enter
Sleep 3500ms

Sleep 2s
