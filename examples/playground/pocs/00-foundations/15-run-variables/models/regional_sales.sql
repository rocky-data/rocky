-- A model parameterised by three per-run variables.
--
--   @var(region)          REQUIRED — no inline default. Must be supplied with
--                         `--var region=<value>` or the compile fails (E028).
--   @var(channel, web)    OPTIONAL — carries an inline default of `web`, used
--                         when `--var channel=...` is not supplied.
--   @var(min_amount, 0)   OPTIONAL — a BARE marker (no surrounding quotes) in
--                         numeric position. `--var min_amount=200` renders
--                         `amount >= 200`; omitted, it falls back to `0`.
--
-- `@var()` resolves at compile/render time, before any SQL is parsed, so a
-- marker works whether it sits inside a string literal (the quoted region /
-- channel filters, where the operator owns the quoting) or bare in numeric
-- position (`amount >= @var(min_amount, 0)`). It is NOT rocky.toml's
-- config-time `${ENV}` interpolation.
SELECT
    region,
    channel,
    customer,
    amount
FROM raw__sales.sales
WHERE region  = '@var(region)'
  AND channel = '@var(channel, web)'
  AND amount >= @var(min_amount, 0)
