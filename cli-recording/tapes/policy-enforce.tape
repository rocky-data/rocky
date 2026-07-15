# policy-enforce — the apply-time enforcement seam, creds-free: an agent
# edits a contracted model, plans the change, and `rocky apply` refuses it
# with the rule named; `rocky audit` shows the decision in the ledger.
# Matches README alt-text: "an agent's change to a contracted model is
# planned, rocky apply run as the agent principal is denied by the policy
# plane with the rule named, and rocky audit shows the recorded decision".
# Render with:  ./record.sh policy-enforce   (runs prepare.sh first)

Output out/policy-enforce.gif

Set Shell bash
Set FontSize 18
Set Width 1200
Set Height 700
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

# Quiet setup: enter the prepared scratch dir (git baseline already
# committed by prepare.sh), suppress engine info logs and the deprecation
# banner, and use a minimal "> " prompt. None of this shows.
Hide
Type "cd scratch/policy-enforce" Enter
Type "export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1" Enter
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 600ms

# 1. The agent edits a contracted, gold-layer model.
Type `printf 'SELECT 1 AS order_id, 100 AS amount, 0 AS discount\n' > models/fct_orders.sql` Sleep 400ms Enter
Sleep 800ms

# 2. Plan the change as the agent principal.
Type "PLAN=$(rocky -o json plan --principal agent --base HEAD --model fct_orders | grep -oE '[0-9a-f]{64}' | head -1); echo $PLAN" Sleep 400ms Enter
Sleep 2500ms

# 3. The agent tries to apply. The policy plane refuses, naming the rule.
Type "ROCKY_PRINCIPAL=agent rocky apply $PLAN" Sleep 400ms Enter
Sleep 4s

Type 'echo "exit: $?"' Sleep 300ms Enter
Sleep 1500ms

# 4. The refusal is a recorded decision, not a log line.
Type "rocky audit --output table" Sleep 400ms Enter
Sleep 4s
