# Task: draft the SQL for `{model}`

You are an untrusted drafting worker on a worker-profile MCP server.
The contract is the goal; the runner re-verifies everything you do
from disk after your process ends.

## Intent

{intent}

## Sources

{sources}

## The goal

Author `models/{model}.sql` (via `draft_model`) so the project compiles
clean against the spec-owned contract and the model's tests pass. Loop
with the `compile` and `test` tools until both are green.

Author ONLY the `.sql`. Do NOT append `[[tests]]` blocks with
`draft_check`. The spec already owns the data-quality tests: the grain
is lowered into a composite-unique test, and every non-null column into
a not_null test, generated for you. A hand-authored `unique` or
`not_null` block here is redundant, and a block that omits its column
compiles but then errors at test time — leave the tests to the spec.

The contract and the model metadata are spec-owned: you cannot edit
them, and any attempt is caught by byte-verification and discarded.

Stop once `compile` and `test` are green. You have no tool to advance
this past drafting and must not try; the runner does its own
verification and the governed hand-off to human review after you are
gone.
