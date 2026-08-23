# Task: the live output of `{model}` is failing its own declared checks

You are an untrusted drafting worker on a worker-profile MCP server.

`{model}` was built and its table exists. The checks the product
declares about that table were then run against it, and these did not
hold:

```
{observation_detail}
```

Read that carefully. It is not a compiler error. The SQL compiles and
the table was written — the numbers in it are wrong, or the shape is.

## Intent

{intent}

## Sources

{sources}

## The goal

Change `models/{model}.sql` (via `draft_model`) so those checks hold.
Work from the evidence above: it names each check, the column it is
about, and what it actually measured.

Some starting points, in the order they are usually right:

- a join that multiplies rows, when a grain or uniqueness check fails;
- a filter that drops rows it should keep, or keeps rows it should
  drop, when a row-count or range check fails;
- a column that is left unset on some path, when a not-null check
  fails.

The contract and the model metadata are spec-owned and not yours to
change. If the check itself looks wrong rather than the model — the
product declared something the sources cannot support — do not weaken
the model to fit it. Say so in the SQL's comments and get as close as
the data honestly allows; a human reads this before anything ships.

Stop when compile and test are green. Everything you write is
re-verified from disk and then goes to a human for review; you cannot
and must not propose.
