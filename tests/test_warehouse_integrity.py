"""Warehouse / OLAP integrity tests.

Run against a populated warehouse (set WAREHOUSE_TEST_DSN, e.g.
"host=localhost port=5432 dbname=warehouse user=warehouse password=warehouse").
Skipped when no warehouse is reachable so the suite still runs in a bare env.
"""
import os

import pytest

psycopg2 = pytest.importorskip("psycopg2")

DSN = os.getenv("WAREHOUSE_TEST_DSN")


@pytest.fixture(scope="module")
def conn():
    if not DSN:
        pytest.skip("WAREHOUSE_TEST_DSN not set — skipping warehouse integration tests")
    try:
        c = psycopg2.connect(DSN)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"warehouse unreachable: {exc}")
    yield c
    c.close()


def _scalar(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def test_facts_populated(conn):
    assert _scalar(conn, "SELECT count(*) FROM dw.fact_orders") > 0
    assert _scalar(conn, "SELECT count(*) FROM dw.fact_order_items") > 0


def test_no_orphan_order_items(conn):
    orphans = _scalar(conn, """
        SELECT count(*) FROM dw.fact_order_items fi
        LEFT JOIN dw.dim_product p USING(product_key)
        WHERE p.product_key IS NULL""")
    assert orphans == 0


def test_cube_grand_total_matches_leaf_sum(conn):
    grand = _scalar(conn, "SELECT revenue FROM dw.cube_revenue_time_category WHERE grp_flag=15")
    leaf = _scalar(conn, "SELECT COALESCE(SUM(revenue),0) FROM dw.cube_revenue_time_category WHERE grp_flag=0")
    assert abs(float(grand) - float(leaf)) < 0.01


def test_status_cube_cancel_rate_in_range(conn):
    rate = _scalar(conn, "SELECT cancel_rate_pct FROM dw.cube_orders_status WHERE grp_flag=3")
    assert 0 <= float(rate) <= 100


def test_no_negative_net_amounts(conn):
    assert _scalar(conn, "SELECT count(*) FROM dw.fact_orders WHERE net_amount < 0") == 0
