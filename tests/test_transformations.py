"""Unit tests for spark.lib.transforms on a local SparkSession.

These exercise the real dimension/fact logic with no database, lake or network.
Skipped automatically if pyspark/Java aren't available.
"""
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from spark.lib import transforms as T  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.appName("tests").master("local[1]")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


@pytest.fixture
def sources(spark):
    locations = spark.createDataFrame(
        [("loc-001", "San Jose", "San Jose", "Carmen", "Centro", 9.93, -84.08),
         ("loc-002", "Cartago", "Cartago", "Oriental", "Cartago", 9.86, -83.91)],
        ["location_id", "province", "canton", "district", "zone", "latitude", "longitude"])
    products = spark.createDataFrame(
        [("prod-0001", "Casado con pollo", "Casados", 3800, True),
         ("prod-0002", "Cafe chorreado", "Bebidas", 1200, True)],
        ["product_id", "name", "category", "price", "is_active"])
    users = spark.createDataFrame(
        [("user-00001", "Ana", "ana@x.cr", "2024-01-10", "loc-001", "", 9.93, -84.08),
         ("user-00002", "Beto", "beto@x.cr", "2024-02-20", "loc-002", "user-00001", 9.86, -83.91)],
        ["user_id", "full_name", "email", "signup_date", "location_id",
         "referred_by", "latitude", "longitude"])
    orders = spark.createDataFrame(
        [("ord-1", "user-00001", "loc-001", "2024-03-01 12:30:00", "delivered",
          5000.0, 0.0, 5000.0, 2, 1.5),
         ("ord-2", "user-00002", "loc-002", "2024-03-02 19:45:00", "cancelled",
          3800.0, 0.0, 3800.0, 1, 22.0)],
        ["order_id", "user_id", "location_id", "created_at", "status",
         "gross_amount", "discount_amount", "net_amount", "item_count", "delivery_km"])
    items = spark.createDataFrame(
        [("oi-1", "ord-1", "prod-0001", 1, 3800, 3800),
         ("oi-2", "ord-1", "prod-0002", 1, 1200, 1200),
         ("oi-3", "ord-2", "prod-0001", 1, 3800, 3800)],
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_amount"])
    reservations = spark.createDataFrame(
        [("res-1", "user-00001", "loc-001", "2024-03-03 20:00:00", 4, "honored"),
         ("res-2", "user-00002", "loc-002", "2024-03-04 13:00:00", 2, "no_show")],
        ["reservation_id", "user_id", "location_id", "reserved_at", "party_size", "status"])
    return dict(locations=locations, products=products, users=users,
                orders=orders, items=items, reservations=reservations)


def test_dim_date_keys_and_spanish_names(spark, sources):
    d = T.build_dim_date(sources["orders"], sources["reservations"]).collect()
    by_key = {r["date_key"]: r for r in d}
    assert 20240301 in by_key
    row = by_key[20240301]
    assert row["month_name"] == "marzo"
    assert row["month_label"] == "2024-03"
    assert row["day_of_week"] in range(1, 8)


def test_dim_time_has_24_hours_with_dayparts():
    rows = T.build_dim_time()
    assert len(rows) == 24
    dayparts = {r[3] for r in rows}
    assert {"madrugada", "manana", "tarde", "noche"} <= dayparts


def test_dim_user_resolves_location_surrogate_key(spark, sources):
    dim_loc = T.build_dim_location(sources["locations"])
    dim_user = T.build_dim_user(sources["users"], dim_loc)
    rows = {r["user_id"]: r for r in dim_user.collect()}
    assert rows["user-00001"]["location_key"] is not None
    assert dim_user.count() == 2


def test_fact_order_items_resolves_all_keys_and_amounts(spark, sources):
    dim_loc = T.build_dim_location(sources["locations"])
    dim_user = T.build_dim_user(sources["users"], dim_loc)
    dim_prod = T.build_dim_product(sources["products"])
    dim_status = spark.createDataFrame(
        T.build_dim_order_status(),
        ["status_key", "status", "is_completed", "is_cancelled"])
    fi = T.build_fact_order_items(sources["items"], sources["orders"],
                                  dim_user, dim_prod, dim_loc, dim_status)
    rows = fi.collect()
    assert len(rows) == 3
    assert all(r["product_key"] is not None for r in rows)
    assert all(r["date_key"] is not None for r in rows)
    total = sum(float(r["line_amount"]) for r in rows)
    assert total == pytest.approx(3800 + 1200 + 3800)


def test_fact_orders_flags_completed_and_cancelled(spark, sources):
    dim_loc = T.build_dim_location(sources["locations"])
    dim_user = T.build_dim_user(sources["users"], dim_loc)
    dim_status = spark.createDataFrame(
        T.build_dim_order_status(),
        ["status_key", "status", "is_completed", "is_cancelled"])
    fo = {r["order_id"]: r for r in
          T.build_fact_orders(sources["orders"], sources["users"],
                              dim_user, dim_loc, dim_status).collect()}
    assert fo["ord-1"]["is_completed"] is True
    assert fo["ord-2"]["is_cancelled"] is True
