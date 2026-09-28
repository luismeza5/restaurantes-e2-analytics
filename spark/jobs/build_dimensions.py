"""Job 2/5 — DIMENSIONS: bronze -> silver conformed dimensions.

Builds dim_date, dim_time, dim_location, dim_product, dim_user, dim_order_status
using the pure functions in spark.lib.transforms, then persists them to the
silver lake zone. dim_user/dim_location are written with their natural-id helper
column so the fact job can resolve foreign keys; that helper is dropped before
the warehouse load.
"""
from __future__ import annotations

from spark.lib import transforms as T
from spark.lib.io import read_lake, write_lake
from spark.lib.spark_session import get_spark


def main():
    spark = get_spark("dimensions")

    orders = read_lake(spark, "bronze", "orders")
    reservations = read_lake(spark, "bronze", "reservations")
    locations = read_lake(spark, "bronze", "locations")
    products = read_lake(spark, "bronze", "products")
    users = read_lake(spark, "bronze", "users")

    dim_date = T.build_dim_date(orders, reservations)
    dim_time = spark.createDataFrame(
        T.build_dim_time(), ["time_key", "hour", "hour_label", "daypart"])
    dim_location = T.build_dim_location(locations)
    dim_product = T.build_dim_product(products)
    dim_user = T.build_dim_user(users, dim_location)
    dim_status = spark.createDataFrame(
        T.build_dim_order_status(),
        ["status_key", "status", "is_completed", "is_cancelled"])

    write_lake(dim_date, "silver", "dim_date")
    write_lake(dim_time, "silver", "dim_time")
    write_lake(dim_location, "silver", "dim_location")
    write_lake(dim_product, "silver", "dim_product")
    write_lake(dim_user, "silver", "dim_user")
    write_lake(dim_status, "silver", "dim_order_status")

    print(f"[dimensions] dates={dim_date.count()} locations={dim_location.count()} "
          f"products={dim_product.count()} users={dim_user.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
