# policy-deny — the agent policy plane, creds-free: a deny on a contracted
# model, the pinned scenarios green, then a loosened rule caught in CI.
# Matches README alt-text: "rocky policy check denies an agent apply on a
# contracted model, rocky policy test passes the pinned scenarios, then
# catches a loosened rule in a bad config and exits non-zero".
# Render with:  ./record.sh policy-deny   (runs prepare.sh first)

Output out/policy-deny.gif

Set Shell bash
Set FontSize 18
Set Width 1200
Set Height 700
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

# Quiet setup: enter the clean scratch dir, suppress engine info logs and the
# run-deprecation banner, and use a minimal "> " prompt. None of this shows.
Hide
Type "cd scratch/policy-deny" Enter
Type "export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1" Enter
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 600ms

# 1. An agent wants to apply to a contracted model. The policy plane says no.
Type "rocky policy check --principal agent --capability apply --model fct_revenue --output table" Sleep 400ms Enter
Sleep 4s

# 2. The policy itself is pinned by scenarios — all green.
Type "rocky policy test --output table" Sleep 400ms Enter
Sleep 4s

# 3. A careless edit loosened the contract-boundary deny to allow.
#    The pinned scenario catches it and the command exits non-zero.
Type "rocky -c rocky-hole.toml policy test --output table" Sleep 400ms Enter
Sleep 3500ms

Type 'echo "exit: $?"' Sleep 300ms Enter
Sleep 3s
