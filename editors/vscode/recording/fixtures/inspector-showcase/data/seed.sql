-- Deterministic SaaS-analytics seed for the Rocky Inspector demo recording.
--
-- Everything is derived from the generate_series index (no random()), so the
-- materialized tables, the [[tests]], and the Profile/Preview tabs are
-- byte-stable across runs.

CREATE SCHEMA IF NOT EXISTS demo;

-- Make `demo` the connection default schema. The file-DB `rocky run` path is
-- unaffected (it materializes via fully-qualified poc.demo.*), but the
-- in-memory `rocky test` path materializes each model output as a bare
-- `CREATE TABLE <model_name>` on this same connection — `USE demo` lands those
-- outputs in `demo` so model->model refs (`demo.dim_customers`, ...) resolve.
USE demo;

-- 50 customers. full_name / email / tax_id / country are all deterministic
-- functions of the index. Country cycles across 5 regions.
CREATE OR REPLACE TABLE demo.seed_customers AS
SELECT
    i                                              AS customer_id,
    'Customer ' || lpad(i::TEXT, 3, '0')           AS full_name,
    'customer' || i || '@example.com'              AS email,
    'TAX-' || lpad(i::TEXT, 6, '0')                AS tax_id,
    (ARRAY['US', 'GB', 'DE', 'FR', 'CA'])[1 + (i % 5)] AS country,
    DATE '2023-01-01' + (i * 5)::INTEGER           AS signup_date
FROM generate_series(1, 50) AS t(i);

-- 320 orders spread across 50 customers. ~1 in 9 is 'cancelled' (filtered
-- out by stg_orders); the rest cycle pending/shipped/delivered. amount_cents
-- is an integer derived from the index. Orders map to customers 1..45, so
-- customers 46..50 have no orders and fall into the 'new' segment downstream.
CREATE OR REPLACE TABLE demo.seed_orders AS
SELECT
    i                                              AS order_id,
    1 + (i % 45)                                   AS customer_id,
    (2500 + (i % 40) * 175)                        AS amount_cents,
    CASE
        WHEN i % 9 = 0 THEN 'cancelled'
        WHEN i % 3 = 0 THEN 'pending'
        WHEN i % 3 = 1 THEN 'shipped'
        ELSE 'delivered'
    END                                            AS status,
    (TIMESTAMP '2024-01-01 09:00:00' + (i % 120) * INTERVAL '1 day') AS ordered_at
FROM generate_series(1, 320) AS t(i);

-- 220 payments across customers 1..45 (matching the order population). ~1 in
-- 11 is a 'refund' (filtered out by stg_payments); the rest cycle
-- card/bank_transfer/paypal.
CREATE OR REPLACE TABLE demo.seed_payments AS
SELECT
    i                                              AS payment_id,
    1 + (i % 45)                                   AS customer_id,
    (3000 + (i % 30) * 210)                        AS amount_cents,
    CASE
        WHEN i % 11 = 0 THEN 'refund'
        WHEN i % 3 = 0 THEN 'card'
        WHEN i % 3 = 1 THEN 'bank_transfer'
        ELSE 'paypal'
    END                                            AS method,
    (TIMESTAMP '2024-01-05 12:00:00' + (i % 120) * INTERVAL '1 day') AS paid_at
FROM generate_series(1, 220) AS t(i);
