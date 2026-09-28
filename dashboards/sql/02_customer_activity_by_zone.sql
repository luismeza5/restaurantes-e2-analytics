-- Dashboard 2 · "Actividad de clientes por zona geográfica"
-- Virtual dataset for Superset. Combines the customer-activity cube (active
-- customers / orders per zone per month) with the location cube for a map-ready
-- revenue-by-zone layer.
--
-- Suggested charts:
--   * Bar: active_customers by zone
--   * Heatmap: zone (y) x month_label (x) coloured by orders
--   * Table: zone, active_customers, orders_per_customer, revenue_per_customer
SELECT
    zone,
    province,
    month_label,
    active_customers,
    orders,
    orders_per_customer,
    revenue,
    revenue_per_customer
FROM dw.cube_customer_activity
WHERE grp_flag = 0            -- (zone, province, month) leaf grain
ORDER BY zone, month_label;
