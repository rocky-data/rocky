-- Seeds the persistent warehouse (wh.duckdb). Inside wh.duckdb the database is
-- `wh`, so `raw.stripe_charges` here IS `wh.raw.stripe_charges` to the model.
-- Row-identical to data/seed.sql.
--
-- Every "recent" row shares ONE offset and the older row is exactly 24h behind
-- it, so the two calendar days differ at any hour of the night. The previous
-- seed spread the recent rows over -1h/-2h/-3h and put the old row at -1 day:
-- run the POC before ~03:00 local and all five collapsed onto the SAME calendar
-- day, leaving 2 distinct client/day pairs instead of 3. The grain check then
-- proved nothing and run.sh's own vacuity guard failed the run (#1526).
--
-- The 3-hour margin is load-bearing twice over. It keeps MAX(loaded_at) far
-- enough in the past that the freshness lag stays POSITIVE — DuckDB's now() is
-- local while the engine computes lag in UTC, so seeding at exactly now()
-- reports a negative lag, which run.sh's `lag ([0-9]+)s` capture cannot read.
-- And 3h is well inside the 86400s freshness budget, so the fresh case still
-- passes before assert 10 deliberately backdates the output.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.stripe_charges (
  client_id  BIGINT,
  charged_at TIMESTAMP,
  amount_eur DOUBLE,
  is_refund  BOOLEAN
);
DELETE FROM raw.stripe_charges;
INSERT INTO raw.stripe_charges VALUES
  (1, now()::TIMESTAMP - INTERVAL '3 hours',  100.0, false),
  (1, now()::TIMESTAMP - INTERVAL '3 hours',   50.0, false),
  (1, now()::TIMESTAMP - INTERVAL '27 hours',  40.0, false),
  (2, now()::TIMESTAMP - INTERVAL '3 hours',   75.0, false),
  (2, now()::TIMESTAMP - INTERVAL '3 hours',   10.0, true);
