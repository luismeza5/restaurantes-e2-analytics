-- Dashboard 1 · "Ingresos por mes y categoría de producto"
-- Virtual dataset for Superset. Reads the OLAP cube at the (month x category)
-- leaf grain (grp_flag = 0 => no sub-totals), so charts aggregate cleanly.
--
-- Suggested charts on this dataset:
--   * Stacked bar: month_label (x) x revenue (y) broken down by category
--   * Line: revenue trend per category
--   * Big number: total revenue (sum)
SELECT
    month_label,
    year,
    quarter_label,
    category,
    revenue,
    units_sold,
    orders,
    avg_ticket
FROM dw.cube_revenue_time_category
WHERE grp_flag = 0           -- leaf rows only (year+quarter+month+category)
ORDER BY month_label, category;
