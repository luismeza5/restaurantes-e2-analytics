-- =============================================================================
-- restaurantes-e2-analytics · OLAP layer · 6 cubes (>= 5 required)
-- =============================================================================
-- Each cube is a MATERIALIZED VIEW built with ROLLUP / CUBE / GROUPING SETS so
-- that a single relation contains every aggregation level (sub-totals and grand
-- totals), which is exactly what an OLAP cube exposes to a BI tool. The
-- GROUPING() flags let dashboards tell a sub-total row apart from a leaf row.
--
-- Coverage required by the rubric: time, location, product type, frequency.
--   cube_revenue_time_category .... time x product type (ROLLUP over Y/Q/M)
--   cube_sales_by_location ........ location           (ROLLUP over prov/cant/dist)
--   cube_product_frequency ........ product type + frequency of use
--   cube_orders_status ............ completed vs cancelled (CUBE)
--   cube_peak_hours ............... time (day-of-week x hour)
--   cube_customer_activity ........ location + frequency (active customers/zone)
--
-- Refreshed by Airflow after each load via dw.refresh_cubes().
-- =============================================================================

SET search_path TO dw, public;

-- 1) Revenue by time (Year > Quarter > Month, ROLLUP) x product category --------
DROP MATERIALIZED VIEW IF EXISTS dw.cube_revenue_time_category CASCADE;
CREATE MATERIALIZED VIEW dw.cube_revenue_time_category AS
SELECT
    d.year,
    d.quarter_label,
    d.month_label,
    p.category,
    GROUPING(d.year, d.quarter_label, d.month_label, p.category) AS grp_flag,
    SUM(fi.line_amount)                AS revenue,
    SUM(fi.quantity)                   AS units_sold,
    COUNT(DISTINCT fi.order_id)        AS orders,
    ROUND(SUM(fi.line_amount) / NULLIF(COUNT(DISTINCT fi.order_id),0), 2) AS avg_ticket
FROM dw.fact_order_items fi
JOIN dw.dim_date    d ON d.date_key    = fi.date_key
JOIN dw.dim_product p ON p.product_key = fi.product_key
JOIN dw.dim_order_status s ON s.status_key = fi.status_key
WHERE s.is_cancelled = FALSE
GROUP BY GROUPING SETS (
    (d.year, d.quarter_label, d.month_label, p.category),  -- leaf
    (d.year, d.quarter_label, d.month_label),              -- month sub-total
    (d.year, d.quarter_label),                             -- quarter sub-total
    (d.year),                                              -- year sub-total
    (p.category),                                          -- category grand
    ()                                                     -- grand total
);

-- 2) Sales by geography (Province > Canton > District, ROLLUP) ------------------
DROP MATERIALIZED VIEW IF EXISTS dw.cube_sales_by_location CASCADE;
CREATE MATERIALIZED VIEW dw.cube_sales_by_location AS
SELECT
    l.province,
    l.canton,
    l.district,
    l.zone,
    GROUPING(l.province, l.canton, l.district) AS grp_flag,
    COUNT(*)                          AS orders,
    SUM(fo.net_amount)                AS revenue,
    SUM(fo.item_count)                AS units,
    ROUND(AVG(fo.net_amount), 2)      AS avg_order_value,
    ROUND(AVG(fo.delivery_km)::numeric, 2) AS avg_delivery_km
FROM dw.fact_orders fo
JOIN dw.dim_location l ON l.location_key = fo.location_key
WHERE fo.is_cancelled = FALSE
GROUP BY ROLLUP (l.province, l.canton, l.district), l.zone;

-- 3) Product purchase frequency / popularity (product type + frequency) ---------
DROP MATERIALIZED VIEW IF EXISTS dw.cube_product_frequency CASCADE;
CREATE MATERIALIZED VIEW dw.cube_product_frequency AS
SELECT
    p.category,
    p.product_name,
    d.month_label,
    GROUPING(p.category, p.product_name, d.month_label) AS grp_flag,
    SUM(fi.quantity)             AS units_sold,
    COUNT(DISTINCT fi.order_id)  AS times_ordered,      -- "frecuencia de uso"
    COUNT(DISTINCT fi.user_key)  AS distinct_buyers,
    SUM(fi.line_amount)          AS revenue
FROM dw.fact_order_items fi
JOIN dw.dim_product p ON p.product_key = fi.product_key
JOIN dw.dim_date    d ON d.date_key    = fi.date_key
JOIN dw.dim_order_status s ON s.status_key = fi.status_key
WHERE s.is_cancelled = FALSE
GROUP BY GROUPING SETS (
    (p.category, p.product_name, d.month_label),
    (p.category, p.product_name),
    (p.category, d.month_label),
    (p.category),
    ()
);

-- 4) Orders completed vs cancelled (CUBE over month x location) -----------------
DROP MATERIALIZED VIEW IF EXISTS dw.cube_orders_status CASCADE;
CREATE MATERIALIZED VIEW dw.cube_orders_status AS
SELECT
    d.month_label,
    l.zone,
    GROUPING(d.month_label, l.zone) AS grp_flag,
    COUNT(*)                                                   AS total_orders,
    COUNT(*) FILTER (WHERE fo.is_completed)                    AS completed,
    COUNT(*) FILTER (WHERE fo.is_cancelled)                    AS cancelled,
    ROUND(100.0 * COUNT(*) FILTER (WHERE fo.is_cancelled)
          / NULLIF(COUNT(*),0), 2)                             AS cancel_rate_pct,
    SUM(fo.net_amount) FILTER (WHERE fo.is_completed)          AS realised_revenue,
    SUM(fo.gross_amount) FILTER (WHERE fo.is_cancelled)        AS lost_revenue
FROM dw.fact_orders fo
JOIN dw.dim_date     d ON d.date_key     = fo.date_key
JOIN dw.dim_location l ON l.location_key = fo.location_key
GROUP BY CUBE (d.month_label, l.zone);

-- 5) Peak hours (time: day-of-week x hour-of-day) -------------------------------
DROP MATERIALIZED VIEW IF EXISTS dw.cube_peak_hours CASCADE;
CREATE MATERIALIZED VIEW dw.cube_peak_hours AS
SELECT
    d.day_of_week,
    d.day_name,
    t.hour,
    t.daypart,
    GROUPING(d.day_of_week, t.hour) AS grp_flag,
    COUNT(*)            AS orders,
    SUM(fo.net_amount)  AS revenue,
    ROUND(AVG(fo.item_count), 2) AS avg_items
FROM dw.fact_orders fo
JOIN dw.dim_date d ON d.date_key = fo.date_key
JOIN dw.dim_time t ON t.time_key = fo.time_key
WHERE fo.is_cancelled = FALSE
GROUP BY GROUPING SETS (
    (d.day_of_week, d.day_name, t.hour, t.daypart),
    (t.hour, t.daypart),
    (d.day_of_week, d.day_name),
    (t.daypart),
    ()
);

-- 6) Customer activity by zone (location + frequency of use) --------------------
DROP MATERIALIZED VIEW IF EXISTS dw.cube_customer_activity CASCADE;
CREATE MATERIALIZED VIEW dw.cube_customer_activity AS
SELECT
    l.zone,
    l.province,
    d.month_label,
    GROUPING(l.zone, l.province, d.month_label) AS grp_flag,
    COUNT(DISTINCT fo.user_key)                          AS active_customers,
    COUNT(*)                                             AS orders,
    ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT fo.user_key),0), 2) AS orders_per_customer,
    SUM(fo.net_amount)                                   AS revenue,
    ROUND(SUM(fo.net_amount) / NULLIF(COUNT(DISTINCT fo.user_key),0), 2) AS revenue_per_customer
FROM dw.fact_orders fo
JOIN dw.dim_location l ON l.location_key = fo.location_key
JOIN dw.dim_date     d ON d.date_key     = fo.date_key
WHERE fo.is_cancelled = FALSE
GROUP BY GROUPING SETS (
    (l.zone, l.province, d.month_label),
    (l.zone, d.month_label),
    (l.zone),
    (d.month_label),
    ()
);

-- -----------------------------------------------------------------------------
-- Unique indexes => enable REFRESH MATERIALIZED VIEW CONCURRENTLY
-- -----------------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS ux_cube_rev_time_cat
  ON dw.cube_revenue_time_category (year, quarter_label, month_label, category, grp_flag);
CREATE UNIQUE INDEX IF NOT EXISTS ux_cube_sales_loc
  ON dw.cube_sales_by_location (province, canton, district, zone, grp_flag);
CREATE UNIQUE INDEX IF NOT EXISTS ux_cube_prod_freq
  ON dw.cube_product_frequency (category, product_name, month_label, grp_flag);
CREATE UNIQUE INDEX IF NOT EXISTS ux_cube_orders_status
  ON dw.cube_orders_status (month_label, zone, grp_flag);
CREATE UNIQUE INDEX IF NOT EXISTS ux_cube_peak_hours
  ON dw.cube_peak_hours (day_of_week, hour, daypart, grp_flag);
CREATE UNIQUE INDEX IF NOT EXISTS ux_cube_cust_act
  ON dw.cube_customer_activity (zone, province, month_label, grp_flag);

-- -----------------------------------------------------------------------------
-- One call to refresh every cube (used by the Airflow load task).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dw.refresh_cubes() RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY dw.cube_revenue_time_category;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dw.cube_sales_by_location;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dw.cube_product_frequency;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dw.cube_orders_status;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dw.cube_peak_hours;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dw.cube_customer_activity;
EXCEPTION WHEN feature_not_supported OR object_not_in_prerequisite_state THEN
    -- first build: CONCURRENTLY needs an existing populated MV, so fall back.
    REFRESH MATERIALIZED VIEW dw.cube_revenue_time_category;
    REFRESH MATERIALIZED VIEW dw.cube_sales_by_location;
    REFRESH MATERIALIZED VIEW dw.cube_product_frequency;
    REFRESH MATERIALIZED VIEW dw.cube_orders_status;
    REFRESH MATERIALIZED VIEW dw.cube_peak_hours;
    REFRESH MATERIALIZED VIEW dw.cube_customer_activity;
END;
$$ LANGUAGE plpgsql;
