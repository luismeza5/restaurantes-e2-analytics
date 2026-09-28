"""I/O helpers: source readers + lake/warehouse writers.

Source is pluggable via SOURCE_KIND:
  csv      -> read data/source/*.csv produced by data/generate_source.py (default)
  postgres -> read Proyecto 1's OLTP Postgres directly (SOURCE_JDBC_URL)
  mongo    -> read Proyecto 1's MongoDB (requires the mongo-spark connector)

This is the single seam where "extract from MongoDB or PostgreSQL" (Airflow
requirement 4) is satisfied — the rest of the pipeline never knows the origin.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from spark.lib.spark_session import lake_path, pg_props

SOURCE_TABLES = ["locations", "categories", "products", "users",
                 "orders", "order_items", "reservations"]


def read_source(spark: SparkSession, table: str) -> DataFrame:
    kind = os.getenv("SOURCE_KIND", "csv")
    if kind == "csv":
        path = os.path.join(os.getenv("SOURCE_PATH", "data/source"), f"{table}.csv")
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    if kind == "postgres":
        return (
            spark.read.format("jdbc")
            .option("url", os.environ["SOURCE_JDBC_URL"])
            .option("dbtable", os.getenv(f"SOURCE_TABLE_{table.upper()}", table))
            .option("user", os.getenv("SOURCE_USER", "app"))
            .option("password", os.getenv("SOURCE_PASSWORD", "app"))
            .option("driver", "org.postgresql.Driver")
            .load()
        )
    if kind == "mongo":
        return (
            spark.read.format("mongodb")
            .option("connection.uri", os.environ["SOURCE_MONGO_URI"])
            .option("database", os.getenv("SOURCE_MONGO_DB", "restaurant"))
            .option("collection", table)
            .load()
        )
    raise ValueError(f"unknown SOURCE_KIND={kind}")


def write_lake(df: DataFrame, zone: str, table: str, partition_by=None) -> None:
    writer = df.write.mode("overwrite").format("parquet")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(lake_path(zone, table))


def read_lake(spark: SparkSession, zone: str, table: str) -> DataFrame:
    return spark.read.parquet(lake_path(zone, table))


def write_warehouse(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    """Load a gold table into the Postgres warehouse (schema dw)."""
    props = pg_props()
    (
        df.write.format("jdbc")
        .option("url", props["url"])
        .option("dbtable", f"dw.{table}")
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .option("truncate", "true")
        .option("isolationLevel", "NONE")
        .mode(mode)
        .save()
    )
