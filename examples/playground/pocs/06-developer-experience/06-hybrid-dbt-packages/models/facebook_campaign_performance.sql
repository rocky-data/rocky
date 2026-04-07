-- Rocky model consuming dbt package tables as external sources.
-- The dbt_fivetran.stg_facebook_ads__* tables are owned by the
-- fivetran/facebook_ads dbt package — Rocky just reads them.

SELECT
    c.campaign_id,
    c.campaign_name,
    c.objective,
    COUNT(DISTINCT r.ad_id) AS active_ads,
    SUM(r.impressions)      AS total_impressions,
    SUM(r.clicks)           AS total_clicks,
    SUM(r.spend)            AS total_spend,
    SUM(r.conversions)      AS total_conversions,
    SUM(r.conversion_value) AS total_conversion_value,
    ROUND(SUM(r.clicks)::DOUBLE / NULLIF(SUM(r.impressions), 0) * 100, 2)          AS ctr_pct,
    ROUND(SUM(r.spend) / NULLIF(SUM(r.conversions), 0), 2)                         AS cost_per_conversion,
    ROUND(SUM(r.conversion_value) / NULLIF(SUM(r.spend), 0), 2)                    AS roas
FROM dbt_fivetran.stg_facebook_ads__campaign_history  c
JOIN dbt_fivetran.stg_facebook_ads__ad_history        a ON a.campaign_id = c.campaign_id
JOIN dbt_fivetran.stg_facebook_ads__ad_report_daily   r ON r.ad_id = a.ad_id
WHERE a.ad_status = 'ACTIVE'
GROUP BY c.campaign_id, c.campaign_name, c.objective
