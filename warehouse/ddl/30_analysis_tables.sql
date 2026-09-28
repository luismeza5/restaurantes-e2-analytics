-- =============================================================================
-- restaurantes-e2-analytics · Analysis result tables (output of spark/jobs/analyses.py)
-- =============================================================================
-- These hold the three required Spark analyses so Superset can chart them and
-- the lightweight loader (scripts/load_local_warehouse.py) has targets to fill.
-- The Spark JDBC writer (spark/jobs/load_warehouse.py) also targets these.
-- =============================================================================
SET search_path TO dw, public;

DROP TABLE IF EXISTS dw.analysis_consumption_trends CASCADE;
CREATE TABLE dw.analysis_consumption_trends (
    category    TEXT,
    month_label TEXT,
    year        SMALLINT,
    month       SMALLINT,
    revenue     NUMERIC(16,2),
    units       BIGINT,
    orders      BIGINT,
    revenue_ma3 NUMERIC(16,2)
);

DROP TABLE IF EXISTS dw.analysis_peak_hours CASCADE;
CREATE TABLE dw.analysis_peak_hours (
    day_of_week SMALLINT,
    day_name    TEXT,
    hour        SMALLINT,
    daypart     TEXT,
    orders      BIGINT,
    revenue     NUMERIC(16,2),
    avg_items   NUMERIC(10,2)
);

DROP TABLE IF EXISTS dw.analysis_monthly_growth CASCADE;
CREATE TABLE dw.analysis_monthly_growth (
    month_label        TEXT,
    year               SMALLINT,
    month              SMALLINT,
    revenue            NUMERIC(16,2),
    orders             BIGINT,
    prev_revenue       NUMERIC(16,2),
    prev_orders        BIGINT,
    revenue_growth_pct NUMERIC(10,2),
    orders_growth_pct  NUMERIC(10,2)
);
