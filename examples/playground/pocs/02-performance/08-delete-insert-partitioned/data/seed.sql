CREATE SCHEMA IF NOT EXISTS seeds;

-- Daily sales data across 3 regions
CREATE OR REPLACE TABLE seeds.daily_sales AS
SELECT
    DATE '2026-04-01' + INTERVAL (i / 10) DAY AS sale_date,
    CASE (i % 3) WHEN 0 THEN 'us_east' WHEN 1 THEN 'us_west' ELSE 'eu_west' END AS region,
    i AS order_id,
    ROUND(10 + RANDOM() * 990, 2) AS amount
FROM generate_series(1, 300) AS t(i);
