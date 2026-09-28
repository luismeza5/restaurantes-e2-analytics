"""Job 3/5 — FACTS: bronze + silver dims -> gold fact tables.

Resolves every business key to its dimension surrogate key and writes the three
facts (fact_orders, fact_order_items, fact_reservations) to the gold zone.
Partitioning by year/month is left out for simplicity and is easy to add.
These are the tables the warehouse load and
the cubes consume.
"""
from __future__ import annotations

from spark.lib import transforms as T
from spark.lib.io import read_lake, write_lake
from spark.lib.spark_session import get_spark


def main():
    spark = get_spark("facts")

    orders = read_lake(spark, "bronze", "orders")
    order_items = read_lake(spark, "bronze", "order_items")
    reservations = read_lake(spark, "bronze", "reservations")
    users = read_lake(spark, "bronze", "users")

    dim_user = read_lake(spark, "silver", "dim_user")
    dim_location = read_lake(spark, "silver", "dim_location")
    dim_product = read_lake(spark, "silver", "dim_product")
    dim_status = read_lake(spark, "silver", "dim_order_status")

    fact_orders = T.build_fact_orders(orders, users, dim_user, dim_location, dim_status)
    fact_items = T.build_fact_order_items(order_items, orders, dim_user, dim_product,
                                          dim_location, dim_status)
    fact_res = T.build_fact_reservations(reservations, dim_user, dim_location)

    write_lake(fact_orders, "gold", "fact_orders")
    write_lake(fact_items, "gold", "fact_order_items")
    write_lake(fact_res, "gold", "fact_reservations")

    print(f"[facts] orders={fact_orders.count()} items={fact_items.count()} "
          f"reservations={fact_res.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
