-- A model parameterised by two per-run variables.
--
--   @var(region)          REQUIRED — no inline default. Must be supplied with
--                         `--var region=<value>` or the compile fails (E028).
--   @var(channel, web)    OPTIONAL — carries an inline default of `web`, used
--                         when `--var channel=...` is not supplied.
--
-- The operator owns the quoting: each marker sits inside a string literal, so
-- `--var region=us` renders `'us'`. `@var()` resolves at compile/render time,
-- before any SQL is parsed — it is NOT rocky.toml's config-time `${ENV}`
-- interpolation.
SELECT
    region,
    channel,
    customer,
    amount
FROM raw__sales.sales
WHERE region  = '@var(region)'
  AND channel = '@var(channel, web)'
