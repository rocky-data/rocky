# column-lineage — trace fct_revenue.total back through the DAG to the seed.
# Matches README alt-text: "rocky lineage --column traces fct_revenue.total
# back to seeds.orders.amount".
# Render with:  ./record.sh column-lineage   (runs prepare.sh first)

Output out/column-lineage.gif

Set Shell bash
Set FontSize 18
Set Width 1200
Set Height 700
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Dracula"

Hide
Type "cd scratch/column-lineage" Enter
Type "export RUST_LOG=error" Enter
Type 'export PS1="> "' Enter
Type "clear" Enter
Show

Sleep 600ms

# 1. Compile — the SELECT * lint shows the schema dependency that lineage resolves.
Type "rocky compile --models models --output table" Sleep 400ms Enter
Sleep 3000ms

# 2. Model-level lineage for fct_revenue.
Type "rocky lineage fct_revenue --models models --output table" Sleep 400ms Enter
Sleep 2500ms

# 3. Column trace — the payoff: total <- raw_orders.amount <- seeds.orders.amount.
Type "rocky lineage fct_revenue --models models --column total --output table" Sleep 400ms Enter
Sleep 3000ms

Sleep 2s
