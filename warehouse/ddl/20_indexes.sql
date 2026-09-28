-- =============================================================================
-- restaurantes-e2-analytics · Warehouse indexes (star-join performance)
-- =============================================================================
SET search_path TO dw, public;

-- Fact foreign-key indexes (star joins hit these constantly).
CREATE INDEX IF NOT EXISTS ix_foi_date     ON dw.fact_order_items(date_key);
CREATE INDEX IF NOT EXISTS ix_foi_product  ON dw.fact_order_items(product_key);
CREATE INDEX IF NOT EXISTS ix_foi_user     ON dw.fact_order_items(user_key);
CREATE INDEX IF NOT EXISTS ix_foi_loc      ON dw.fact_order_items(location_key);
CREATE INDEX IF NOT EXISTS ix_foi_status   ON dw.fact_order_items(status_key);

CREATE INDEX IF NOT EXISTS ix_fo_date      ON dw.fact_orders(date_key);
CREATE INDEX IF NOT EXISTS ix_fo_user      ON dw.fact_orders(user_key);
CREATE INDEX IF NOT EXISTS ix_fo_loc       ON dw.fact_orders(location_key);
CREATE INDEX IF NOT EXISTS ix_fo_status    ON dw.fact_orders(status_key);
CREATE INDEX IF NOT EXISTS ix_fo_time      ON dw.fact_orders(time_key);

CREATE INDEX IF NOT EXISTS ix_fr_date      ON dw.fact_reservations(date_key);
CREATE INDEX IF NOT EXISTS ix_fr_user      ON dw.fact_reservations(user_key);
CREATE INDEX IF NOT EXISTS ix_fr_loc       ON dw.fact_reservations(location_key);

-- Snowflake link user -> location
CREATE INDEX IF NOT EXISTS ix_dim_user_loc ON dw.dim_user(location_key);
