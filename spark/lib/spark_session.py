"""Spark session factory shared by every job.

Two modes, chosen by env so the same jobs run on a laptop and in Docker:

  WAREHOUSE_MODE=lake  -> register tables in a Hive Metastore and write Parquet
                          to LAKE_PATH (the "lakehouse" path). Used in compose.
  WAREHOUSE_MODE=local -> plain Parquet under LAKE_PATH, no metastore. Used for
                          local dev / CI / unit tests (no Hive needed).

The Postgres JDBC driver and (optionally) the Hive metastore connection are
wired here so individual jobs stay clean.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

PG_JDBC_VERSION = "42.7.3"


def get_spark(app_name: str = "restaurantes-analytics") -> SparkSession:
    mode = os.getenv("WAREHOUSE_MODE", "local")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "America/Costa_Rica")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
    )

    # The Postgres JDBC driver is only needed by load_warehouse and is fetched
    # from Maven at startup; skip it for local/CI runs so extract / dimensions /
    # facts / analyses work fully offline.
    if mode == "lake" or os.getenv("SPARK_INCLUDE_JDBC") == "1":
        builder = builder.config("spark.jars.packages",
                                 f"org.postgresql:postgresql:{PG_JDBC_VERSION}")

    # Hive is an OPTIONAL "SQL-on-the-lakehouse" layer. All jobs do path-based
    # Parquet I/O and never need the metastore to run; enable it only when a
    # metastore is actually available (compose `lake` profile sets HIVE_ENABLED=1)
    # so the catalog can expose the gold tables to external Hive/Trino clients.
    if os.getenv("HIVE_ENABLED") == "1":
        metastore_uri = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
        builder = (
            builder.config("hive.metastore.uris", metastore_uri)
            .config("spark.sql.warehouse.dir", os.getenv("LAKE_PATH", "/lake/warehouse"))
            .enableHiveSupport()
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def lake_path(zone: str, table: str) -> str:
    """Parquet location for a (zone, table) in the data lake. zone in bronze/silver/gold."""
    base = os.getenv("LAKE_PATH", "/lake/warehouse").rstrip("/")
    return f"{base}/{zone}/{table}"


def pg_props() -> dict:
    return {
        "url": os.getenv("WAREHOUSE_JDBC_URL",
                         "jdbc:postgresql://warehouse-db:5432/warehouse"),
        "user": os.getenv("WAREHOUSE_USER", "warehouse"),
        "password": os.getenv("WAREHOUSE_PASSWORD", "warehouse"),
        "driver": "org.postgresql.Driver",
    }
