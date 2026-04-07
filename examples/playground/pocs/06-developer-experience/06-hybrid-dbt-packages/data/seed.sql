-- Seed data simulating tables produced by dbt packages.
--
-- In a real warehouse these tables are created and maintained by:
--   - Fivetran connectors (raw data)
--   - dbt packages like fivetran/facebook_ads and fivetran/stripe
--
-- Rocky does NOT manage these tables — it references them as external sources.

------------------------------------------------------------------------
-- 1. Fivetran raw schema (what the Fivetran connector lands)
------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS fivetran_facebook_ads;

CREATE OR REPLACE TABLE fivetran_facebook_ads.ad_history AS
SELECT
    100 + i AS ad_id,
    'Ad creative ' || i AS ad_name,
    CASE i % 3
        WHEN 0 THEN 1001
        WHEN 1 THEN 1002
        ELSE    1003
    END AS campaign_id,
    CASE WHEN i % 5 = 0 THEN 'PAUSED' ELSE 'ACTIVE' END AS status,
    TIMESTAMP '2026-01-01' + INTERVAL (i * 86400) SECOND AS created_at,
    TIMESTAMP '2026-04-01' + INTERVAL (i * 3600) SECOND AS _fivetran_synced
FROM generate_series(1, 20) AS t(i);

CREATE OR REPLACE TABLE fivetran_facebook_ads.campaign_history AS
SELECT
    id AS campaign_id,
    'Campaign ' || CASE id
        WHEN 1001 THEN 'Brand Awareness'
        WHEN 1002 THEN 'Retargeting'
        ELSE          'Prospecting'
    END AS campaign_name,
    CASE id
        WHEN 1001 THEN 'AWARENESS'
        WHEN 1002 THEN 'CONVERSIONS'
        ELSE          'REACH'
    END AS objective,
    'ACTIVE' AS status,
    TIMESTAMP '2025-12-15' AS created_at,
    TIMESTAMP '2026-04-01' AS _fivetran_synced
FROM (VALUES (1001), (1002), (1003)) AS t(id);

------------------------------------------------------------------------
-- 2. dbt package output (what fivetran/facebook_ads staging models produce)
--    Schema: dbt_fivetran.stg_facebook_ads__*
------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS dbt_fivetran;

CREATE OR REPLACE TABLE dbt_fivetran.stg_facebook_ads__ad_history AS
SELECT
    ad_id,
    ad_name,
    campaign_id,
    status AS ad_status,
    created_at,
    _fivetran_synced
FROM fivetran_facebook_ads.ad_history
WHERE _fivetran_synced = (
    SELECT MAX(_fivetran_synced) FROM fivetran_facebook_ads.ad_history ah2
    WHERE ah2.ad_id = ad_history.ad_id
);

CREATE OR REPLACE TABLE dbt_fivetran.stg_facebook_ads__campaign_history AS
SELECT
    campaign_id,
    campaign_name,
    objective,
    status AS campaign_status,
    created_at,
    _fivetran_synced
FROM fivetran_facebook_ads.campaign_history;

CREATE OR REPLACE TABLE dbt_fivetran.stg_facebook_ads__ad_report_daily AS
SELECT
    ad.ad_id,
    CAST(TIMESTAMP '2026-03-01' + INTERVAL (d * 86400) SECOND AS DATE) AS report_date,
    CAST(100 + (random() * 900) AS INTEGER) AS impressions,
    CAST(10 + (random() * 90) AS INTEGER) AS clicks,
    ROUND(CAST(5.0 + random() * 45.0 AS DECIMAL(10,2)), 2) AS spend,
    CAST(random() * 5 AS INTEGER) AS conversions,
    ROUND(CAST(random() * 50.0 AS DECIMAL(10,2)), 2) AS conversion_value
FROM fivetran_facebook_ads.ad_history ad
CROSS JOIN generate_series(1, 30) AS t(d);

------------------------------------------------------------------------
-- 3. Stripe tables (simulating fivetran/stripe dbt package output)
------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS dbt_stripe;

CREATE OR REPLACE TABLE dbt_stripe.stg_stripe__charges AS
SELECT
    'ch_' || LPAD(CAST(i AS VARCHAR), 6, '0') AS charge_id,
    ROUND(CAST(10.0 + random() * 200.0 AS DECIMAL(10,2)), 2) AS amount_cents,
    'usd' AS currency,
    CASE WHEN random() < 0.05 THEN 'failed' ELSE 'succeeded' END AS status,
    'cus_' || LPAD(CAST(1 + (i % 50) AS VARCHAR), 4, '0') AS customer_id,
    CAST(TIMESTAMP '2026-03-01' + INTERVAL (i * 3600) SECOND AS DATE) AS created_date,
    TIMESTAMP '2026-04-01' + INTERVAL (i * 60) SECOND AS _fivetran_synced
FROM generate_series(1, 200) AS t(i);
