-- Daily ad spend trends from dbt package tables.
-- Uses the same external sources as facebook_campaign_performance
-- but at a different grain (daily, per-campaign).

SELECT
    r.report_date,
    c.campaign_name,
    c.objective,
    SUM(r.impressions)      AS impressions,
    SUM(r.clicks)           AS clicks,
    SUM(r.spend)            AS spend,
    SUM(r.conversions)      AS conversions,
    ROUND(SUM(r.clicks)::DOUBLE / NULLIF(SUM(r.impressions), 0) * 100, 2) AS ctr_pct,
    -- 7-day rolling average spend
    ROUND(AVG(SUM(r.spend)) OVER (
        PARTITION BY c.campaign_id
        ORDER BY r.report_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS spend_7d_avg
FROM dbt_fivetran.stg_facebook_ads__ad_report_daily   r
JOIN dbt_fivetran.stg_facebook_ads__ad_history        a ON a.ad_id = r.ad_id
JOIN dbt_fivetran.stg_facebook_ads__campaign_history  c ON c.campaign_id = a.campaign_id
GROUP BY r.report_date, c.campaign_id, c.campaign_name, c.objective
