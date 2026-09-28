-- Dashboard 3 · "Pedidos completados vs cancelados"
-- Virtual dataset for Superset built on the status CUBE. Uses the
-- (month x zone) leaf grain (grp_flag = 0); the cube also stores the grand
-- total (grp_flag = 3) and the per-month / per-zone sub-totals for big numbers.
--
-- Suggested charts:
--   * Grouped bar: month_label x [completed, cancelled]
--   * Line: cancel_rate_pct over month_label
--   * Big number: overall cancel_rate_pct (read grp_flag = 3 row)
SELECT
    month_label,
    zone,
    total_orders,
    completed,
    cancelled,
    cancel_rate_pct,
    realised_revenue,
    lost_revenue
FROM dw.cube_orders_status
WHERE grp_flag = 0
ORDER BY month_label, zone;
