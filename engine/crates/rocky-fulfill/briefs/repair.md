# Task: repair the red verification for `{model}`

You are an untrusted drafting worker on a worker-profile MCP server.
The runner's own verification came back red:

```
{verify_detail}
```

## Intent

{intent}

## The goal

Fix `models/{model}.sql` (via `draft_model`) so `compile` and `test`
read green again. The contract and the model metadata are spec-owned
and not yours to change; if the failure looks like a contract problem,
say so in the SQL's comments and get as close to green as the data
allows.

Stop when compile/test are green; you cannot and must not propose.

Your last `draft_model` call is your hand-off: the runner commits
`models/{model}.sql` and `models/{model}.toml` only if they still match
what that call wrote. Do not touch them by any other route afterwards,
or the round is refused rather than committed.
