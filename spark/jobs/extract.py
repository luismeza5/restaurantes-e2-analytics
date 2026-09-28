"""Job 1/5 — EXTRACT: land the OLTP source as-is into the bronze lake zone.

Reads whatever SOURCE_KIND points at (csv / postgres / mongo) and mirrors each
table to Parquet. Keeping a raw bronze copy makes the pipeline re-runnable and
decouples downstream jobs from the source system's availability.
"""
from __future__ import annotations

from spark.lib.io import SOURCE_TABLES, read_source, write_lake
from spark.lib.spark_session import get_spark


def main():
    spark = get_spark("extract")
    for table in SOURCE_TABLES:
        df = read_source(spark, table)
        write_lake(df, "bronze", table)
        print(f"[extract] bronze/{table}: {df.count()} rows")
    spark.stop()


if __name__ == "__main__":
    main()
