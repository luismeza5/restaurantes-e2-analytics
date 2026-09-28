"""Pure Spark transformation functions.

Everything here takes DataFrames and returns DataFrames with NO I/O, so the unit
tests in tests/test_transformations.py can exercise the real logic on a local
SparkSession without any database, lake or network.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# --- dimensions --------------------------------------------------------------

def build_dim_date(orders: DataFrame, reservations: DataFrame) -> DataFrame:
    """Daily-grain calendar covering every date seen in the source facts."""
    dates = (
        orders.select(F.to_date("created_at").alias("d"))
        .union(reservations.select(F.to_date("reserved_at").alias("d")))
        .where(F.col("d").isNotNull())
        .distinct()
    )
    month_es = _month_name_map()
    day_es = _day_name_map()
    return (
        dates.select(
            (F.date_format("d", "yyyyMMdd").cast("int")).alias("date_key"),
            F.col("d").alias("full_date"),
            F.year("d").alias("year"),
            F.quarter("d").alias("quarter"),
            F.concat(F.lit("Q"), F.quarter("d")).alias("quarter_label"),
            F.month("d").alias("month"),
            month_es[F.month("d")].alias("month_name"),
            F.date_format("d", "yyyy-MM").alias("month_label"),
            F.dayofmonth("d").alias("day"),
            # ISO: 1=Mon..7=Sun
            F.when(F.dayofweek("d") == 1, 7).otherwise(F.dayofweek("d") - 1).alias("day_of_week"),
            day_es[F.dayofweek("d")].alias("day_name"),
            (F.dayofweek("d").isin(1, 7)).alias("is_weekend"),
        )
    )


def build_dim_time() -> "list[tuple]":
    """Hour-of-day dimension rows (0..23). Returned as plain rows; tiny."""
    out = []
    for h in range(24):
        if h < 6:
            dp = "madrugada"
        elif h < 12:
            dp = "manana"
        elif h < 18:
            dp = "tarde"
        else:
            dp = "noche"
        out.append((h, h, f"{h:02d}:00", dp))
    return out  # (time_key, hour, hour_label, daypart)


def build_dim_location(locations: DataFrame) -> DataFrame:
    w = Window.orderBy("location_id")
    return locations.select(
        F.row_number().over(w).alias("location_key"),
        "province", "canton", "district", "zone",
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.col("location_id").alias("_nat_location_id"),
    )


def build_dim_product(products: DataFrame) -> DataFrame:
    w = Window.orderBy("product_id")
    return products.select(
        F.row_number().over(w).alias("product_key"),
        F.col("product_id"),
        F.col("name").alias("product_name"),
        F.col("category"),
        F.col("price").cast("decimal(12,2)").alias("unit_price"),
        F.col("is_active").cast("boolean").alias("is_active"),
    )


def build_dim_user(users: DataFrame, dim_location: DataFrame) -> DataFrame:
    w = Window.orderBy("user_id")
    keyed = users.withColumn("user_key", F.row_number().over(w))
    return (
        keyed.join(dim_location.select("location_key", "_nat_location_id"),
                   keyed.location_id == F.col("_nat_location_id"), "left")
        .select(
            "user_key",
            F.col("user_id"),
            F.col("full_name"),
            F.col("email"),
            F.to_date("signup_date").alias("signup_date"),
            F.col("location_key"),
        )
    )


def build_dim_order_status() -> "list[tuple]":
    # (status_key, status, is_completed, is_cancelled)
    return [
        (1, "pending",   False, False),
        (2, "preparing", False, False),
        (3, "delivered", True,  False),
        (4, "completed", True,  False),
        (5, "cancelled", False, True),
    ]


# --- facts -------------------------------------------------------------------

def build_fact_orders(orders, users, dim_user, dim_location, dim_status) -> DataFrame:
    o = (
        orders
        .withColumn("ts", F.to_timestamp("created_at"))
        .withColumn("date_key", F.date_format("ts", "yyyyMMdd").cast("int"))
        .withColumn("time_key", F.hour("ts").cast("short"))
    )
    o = o.join(dim_user.select("user_key", "user_id"), "user_id", "left")
    o = o.join(dim_location.select("location_key", "_nat_location_id"),
               o.location_id == F.col("_nat_location_id"), "left")
    o = o.join(dim_status.select("status_key", "status",
                                 F.col("is_completed").alias("s_completed"),
                                 F.col("is_cancelled").alias("s_cancelled")),
               "status", "left")
    w = Window.orderBy("order_id")
    return o.select(
        F.row_number().over(w).cast("long").alias("order_key"),
        "order_id", "date_key", "time_key", "user_key", "location_key", "status_key",
        F.col("item_count").cast("int"),
        F.col("gross_amount").cast("decimal(14,2)"),
        F.col("discount_amount").cast("decimal(14,2)"),
        F.col("net_amount").cast("decimal(14,2)"),
        F.col("delivery_km").cast("double"),
        F.col("s_completed").alias("is_completed"),
        F.col("s_cancelled").alias("is_cancelled"),
    )


def build_fact_order_items(order_items, orders, dim_user, dim_product,
                           dim_location, dim_status) -> DataFrame:
    oi = order_items.join(
        orders.select("order_id", "user_id", "location_id", "created_at", "status"),
        "order_id", "inner",
    )
    oi = (
        oi.withColumn("ts", F.to_timestamp("created_at"))
        .withColumn("date_key", F.date_format("ts", "yyyyMMdd").cast("int"))
        .withColumn("time_key", F.hour("ts").cast("short"))
    )
    oi = oi.join(dim_user.select("user_key", "user_id"), "user_id", "left")
    oi = oi.join(dim_product.select("product_key", "product_id"), "product_id", "left")
    oi = oi.join(dim_location.select("location_key", "_nat_location_id"),
                 oi.location_id == F.col("_nat_location_id"), "left")
    oi = oi.join(dim_status.select("status_key", "status"), "status", "left")
    w = Window.orderBy("order_item_id")
    return oi.select(
        F.row_number().over(w).cast("long").alias("order_item_key"),
        "order_id", "date_key", "time_key", "user_key", "product_key",
        "location_key", "status_key",
        F.col("quantity").cast("int"),
        F.col("unit_price").cast("decimal(12,2)"),
        F.col("line_amount").cast("decimal(14,2)"),
    )


def build_fact_reservations(reservations, dim_user, dim_location) -> DataFrame:
    r = (
        reservations
        .withColumn("ts", F.to_timestamp("reserved_at"))
        .withColumn("date_key", F.date_format("ts", "yyyyMMdd").cast("int"))
        .withColumn("time_key", F.hour("ts").cast("short"))
        .withColumn("is_honored", F.col("status") == F.lit("honored"))
        .withColumn("is_no_show", F.col("status") == F.lit("no_show"))
    )
    r = r.join(dim_user.select("user_key", "user_id"), "user_id", "left")
    r = r.join(dim_location.select("location_key", "_nat_location_id"),
               r.location_id == F.col("_nat_location_id"), "left")
    w = Window.orderBy("reservation_id")
    return r.select(
        F.row_number().over(w).cast("long").alias("reservation_key"),
        "reservation_id", "date_key", "time_key", "user_key", "location_key",
        F.col("party_size").cast("int"),
        "is_honored", "is_no_show",
    )


# --- helpers -----------------------------------------------------------------

def _month_name_map():
    names = ["", "enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
             "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
    return F.create_map(*sum(([F.lit(i), F.lit(n)] for i, n in enumerate(names)), []))


def _day_name_map():
    # Spark dayofweek: 1=Sun..7=Sat
    names = {1: "domingo", 2: "lunes", 3: "martes", 4: "miercoles",
             5: "jueves", 6: "viernes", 7: "sabado"}
    return F.create_map(*sum(([F.lit(k), F.lit(v)] for k, v in names.items()), []))
