-- Quick manual checks against the warehouse (psql -f scripts/validate_warehouse.sql).
\echo === row counts ===
SELECT 'fact_orders' t, count(*) FROM dw.fact_orders
UNION ALL SELECT 'fact_order_items', count(*) FROM dw.fact_order_items
UNION ALL SELECT 'fact_reservations', count(*) FROM dw.fact_reservations;

\echo === cube grand total == leaf sum (must match) ===
SELECT (SELECT revenue FROM dw.cube_revenue_time_category WHERE grp_flag=15) AS grand_total,
       (SELECT SUM(revenue) FROM dw.cube_revenue_time_category WHERE grp_flag=0) AS leaf_sum;

\echo === overall cancel rate ===
SELECT total_orders, completed, cancelled, cancel_rate_pct
FROM dw.cube_orders_status WHERE grp_flag=3;

\echo === top 5 peak hours ===
SELECT day_name, hour, orders FROM dw.cube_peak_hours
WHERE grp_flag=0 ORDER BY orders DESC LIMIT 5;
