-- Loaded automatically by `rocky test` into an in-memory DuckDB. The product
-- source is the exact triple `wh.raw.stripe_charges`, so we ATTACH a :memory:
-- database named `wh` and create the source under it. Kept row-identical to
-- data/warehouse_seed.sql so the in-memory test and the warehouse agree.
ATTACH IF NOT EXISTS ':memory:' AS wh;
CREATE SCHEMA IF NOT EXISTS wh.raw;
CREATE TABLE IF NOT EXISTS wh.raw.stripe_charges (
  client_id  BIGINT,
  charged_at TIMESTAMP,
  amount_eur DOUBLE,
  is_refund  BOOLEAN
);
INSERT INTO wh.raw.stripe_charges VALUES
  (1, now()::TIMESTAMP - INTERVAL '2 hours', 100.0, false),
  (1, now()::TIMESTAMP - INTERVAL '1 hour',   50.0, false),
  (1, now()::TIMESTAMP - INTERVAL '1 day',    40.0, false),
  (2, now()::TIMESTAMP - INTERVAL '3 hours',  75.0, false),
  (2, now()::TIMESTAMP - INTERVAL '1 hour',   10.0, true);
