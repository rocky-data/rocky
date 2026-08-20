-- Seeds the persistent warehouse (wh.duckdb). Inside wh.duckdb the database is
-- `wh`, so `raw.stripe_charges` here IS `wh.raw.stripe_charges` to the model.
-- Row-identical to data/seed.sql.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.stripe_charges (
  client_id  BIGINT,
  charged_at TIMESTAMP,
  amount_eur DOUBLE,
  is_refund  BOOLEAN
);
DELETE FROM raw.stripe_charges;
INSERT INTO raw.stripe_charges VALUES
  (1, now()::TIMESTAMP - INTERVAL '2 hours', 100.0, false),
  (1, now()::TIMESTAMP - INTERVAL '1 hour',   50.0, false),
  (1, now()::TIMESTAMP - INTERVAL '1 day',    40.0, false),
  (2, now()::TIMESTAMP - INTERVAL '3 hours',  75.0, false),
  (2, now()::TIMESTAMP - INTERVAL '1 hour',   10.0, true);
