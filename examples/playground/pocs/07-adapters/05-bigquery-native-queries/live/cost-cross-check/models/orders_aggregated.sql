SELECT
    customer_id,
    COUNT(*)    AS order_count,
    SUM(amount) AS revenue
FROM `__GCP_PROJECT__`.`hc_phase21_cost_check`.`orders_src`
GROUP BY customer_id
