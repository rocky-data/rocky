-- Joins Rocky-owned models together to produce a unified view.
-- facebook_campaign_performance and stripe_revenue_daily are Rocky models
-- that themselves consume dbt package tables — Rocky resolves the full DAG.

SELECT
    s.created_date                AS report_date,
    s.revenue_usd                 AS stripe_revenue,
    s.unique_customers            AS paying_customers,
    SUM(f.total_spend)            AS facebook_spend,
    SUM(f.total_conversions)      AS facebook_conversions,
    ROUND(s.revenue_usd - COALESCE(SUM(f.total_spend), 0), 2) AS net_after_ads,
    ROUND(s.revenue_usd / NULLIF(SUM(f.total_spend), 0), 2)   AS revenue_to_ad_ratio
FROM stripe_revenue_daily s
LEFT JOIN facebook_daily_trends f ON f.report_date = s.created_date
GROUP BY s.created_date, s.revenue_usd, s.unique_customers
