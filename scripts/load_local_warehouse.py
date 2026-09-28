"""Load the gold/silver lake into the Postgres warehouse without Spark/JDBC.

A pure pandas + psycopg2 alternative to spark/jobs/load_warehouse.py, handy for
local dev and CI where fetching the Spark JDBC driver from Maven isn't desired.
Reads Parquet from LAKE_PATH, truncates+inserts each table, then refreshes cubes.

    WAREHOUSE_TEST_DSN="host=localhost port=5432 dbname=warehouse user=warehouse password=warehouse" \
    LAKE_PATH=.lake python scripts/load_local_warehouse.py
"""
from __future__ import annotations

import glob
import os

import pandas as pd
import psycopg2
import psycopg2.extras as ex

LAKE = os.getenv("LAKE_PATH", ".lake")
DSN = os.getenv("WAREHOUSE_TEST_DSN",
                "host=localhost port=5432 dbname=warehouse user=warehouse password=warehouse")

DIMENSIONS = ["dim_date", "dim_time", "dim_location", "dim_product",
              "dim_user", "dim_order_status"]
FACTS = ["fact_orders", "fact_order_items", "fact_reservations"]
ANALYSES = ["analysis_consumption_trends", "analysis_peak_hours", "analysis_monthly_growth"]
DROP_COLS = {"dim_location": ["_nat_location_id"]}


def read(zone, table):
    files = glob.glob(f"{LAKE}/{zone}/{table}/*.parquet")
    if not files:
        raise FileNotFoundError(f"no parquet for {zone}/{table} under {LAKE}")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def load(conn, zone, table):
    df = read(zone, table)
    for c in DROP_COLS.get(table, []):
        df = df.drop(columns=[c], errors="ignore")
    cols = list(df.columns)
    vals = [tuple(None if pd.isna(v) else v for v in row)
            for row in df.itertuples(index=False)]
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE dw.{table} CASCADE")
        ex.execute_values(cur, f'INSERT INTO dw.{table} ({",".join(cols)}) VALUES %s', vals)
    print(f"[load] dw.{table}: {len(df)} rows")


def main():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    for t in DIMENSIONS:
        load(conn, "silver", t)
    for t in FACTS:
        load(conn, "gold", t)
    for t in ANALYSES:
        load(conn, "gold", t)
    with conn.cursor() as cur:
        cur.execute("SELECT dw.refresh_cubes();")
    conn.close()
    print("[load] OLAP cubes refreshed")


if __name__ == "__main__":
    main()
