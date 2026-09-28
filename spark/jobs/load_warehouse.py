"""Job 5/5 — LOAD: gold lake -> Postgres warehouse (schema dw), then refresh cubes.

Loads dimensions first (FK targets), then facts, then the analysis tables. After
the JDBC loads it calls dw.refresh_cubes() so every materialized OLAP cube
reflects the new data. Run order matters because of the foreign keys in
warehouse/ddl/00_schema.sql.
"""
from __future__ import annotations

import psycopg2

from spark.lib.io import read_lake, write_warehouse
from spark.lib.spark_session import get_spark, pg_props

DIMENSIONS = ["dim_date", "dim_time", "dim_location", "dim_product",
              "dim_user", "dim_order_status"]
FACTS = ["fact_orders", "fact_order_items", "fact_reservations"]
ANALYSES = ["analysis_consumption_trends", "analysis_peak_hours",
            "analysis_monthly_growth"]

# helper natural-id columns that exist only to resolve FKs in Spark
DROP_COLS = {"dim_location": ["_nat_location_id"]}


def main():
    spark = get_spark("load_warehouse")

    for name in DIMENSIONS:
        zone = "silver"
        df = read_lake(spark, zone, name)
        for c in DROP_COLS.get(name, []):
            if c in df.columns:
                df = df.drop(c)
        write_warehouse(df, name)
        print(f"[load] dw.{name}: {df.count()} rows")

    for name in FACTS:
        df = read_lake(spark, "gold", name)
        write_warehouse(df, name)
        print(f"[load] dw.{name}: {df.count()} rows")

    for name in ANALYSES:
        df = read_lake(spark, "gold", name)
        write_warehouse(df, name)
        print(f"[load] dw.{name}: {df.count()} rows")

    spark.stop()
    refresh_cubes()


def refresh_cubes():
    p = pg_props()
    # jdbc:postgresql://host:port/db -> psycopg2 dsn
    raw = p["url"].replace("jdbc:postgresql://", "")
    hostport, db = raw.split("/", 1)
    host, port = (hostport.split(":") + ["5432"])[:2]
    conn = psycopg2.connect(host=host, port=port, dbname=db,
                            user=p["user"], password=p["password"])
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT dw.refresh_cubes();")
    conn.close()
    print("[load] OLAP cubes refreshed")


if __name__ == "__main__":
    main()
