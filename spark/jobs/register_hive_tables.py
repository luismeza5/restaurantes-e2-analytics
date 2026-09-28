"""Optional — register the lakehouse tables in the Hive Metastore.

Creates a Hive database `restaurant_lake` with EXTERNAL tables pointing at the
gold/silver Parquet produced by the pipeline, so the warehouse is queryable with
HiveQL / Trino / any metastore-aware engine. The Postgres star schema remains the low-latency
serving layer for Superset; Hive is the open SQL-on-lakehouse access path over
the exact same data.

Run only when a metastore is available:
    HIVE_ENABLED=1 SPARK_INCLUDE_JDBC=0 spark-submit spark/jobs/register_hive_tables.py
"""
from __future__ import annotations

import os

from spark.lib.spark_session import get_spark, lake_path

DB = os.getenv("HIVE_LAKE_DB", "restaurant_lake")
GOLD = ["fact_orders", "fact_order_items", "fact_reservations",
        "analysis_consumption_trends", "analysis_peak_hours", "analysis_monthly_growth"]
SILVER = ["dim_date", "dim_time", "dim_location", "dim_product", "dim_user", "dim_order_status"]


def main():
    if os.getenv("HIVE_ENABLED") != "1":
        raise SystemExit("HIVE_ENABLED!=1 — refusing to run without a metastore.")
    spark = get_spark("register-hive")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    for zone, tables in (("silver", SILVER), ("gold", GOLD)):
        for t in tables:
            loc = lake_path(zone, t)
            spark.sql(f"DROP TABLE IF EXISTS {DB}.{t}")
            spark.sql(f"CREATE EXTERNAL TABLE {DB}.{t} USING parquet LOCATION '{loc}'")
            print(f"[hive] registered {DB}.{t} -> {loc}")
    print("[hive] tables:", [r.tableName for r in spark.sql(f"SHOW TABLES IN {DB}").collect()])
    spark.stop()


if __name__ == "__main__":
    main()
