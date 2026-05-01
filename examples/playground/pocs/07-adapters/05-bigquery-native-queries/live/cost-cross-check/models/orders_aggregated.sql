SELECT
    customer_id,
    COUNT(*)    AS order_count,
    SUM(amount) AS revenue
FROM `rocky-sandbox-hc-test-63874`.`hc_phase21_cost_check`.`orders_src`
GROUP BY customer_id
