"""Bootstrap Superset assets via its REST API.

Registers the warehouse database connection and creates one virtual dataset per
file in dashboards/sql/. After running this you only need to build the three
charts/dashboards in the UI (see README.md) — the data sources are ready.

    SUPERSET_URL=http://localhost:8088 SUPERSET_USER=admin SUPERSET_PASSWORD=admin \
        python dashboards/superset/bootstrap_assets.py

Idempotent: skips a dataset if a table/view with the same name already exists.
"""
from __future__ import annotations

import glob
import os
import re

import requests

BASE = os.getenv("SUPERSET_URL", "http://localhost:8088").rstrip("/")
USER = os.getenv("SUPERSET_USER", "admin")
PWD = os.getenv("SUPERSET_PASSWORD", "admin")
DB_NAME = "Restaurantes DW"
DB_URI = os.getenv(
    "WAREHOUSE_SQLALCHEMY_URI",
    "postgresql+psycopg2://warehouse:warehouse@warehouse-db:5432/warehouse",
)
SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")


def session():
    s = requests.Session()
    login = s.post(f"{BASE}/api/v1/security/login", json={
        "username": USER, "password": PWD, "provider": "db", "refresh": True})
    login.raise_for_status()
    token = login.json()["access_token"]
    s.headers.update({"Authorization": f"Bearer {token}"})
    csrf = s.get(f"{BASE}/api/v1/security/csrf_token/")
    if csrf.ok:
        s.headers.update({"X-CSRFToken": csrf.json()["result"],
                          "Referer": BASE})
    return s


def ensure_database(s) -> int:
    existing = s.get(f"{BASE}/api/v1/database/",
                     params={"q": f'(filters:!((col:database_name,opr:eq,value:"{DB_NAME}")))'})
    rows = existing.json().get("result", [])
    if rows:
        print(f"[superset] database '{DB_NAME}' already exists (id={rows[0]['id']})")
        return rows[0]["id"]
    resp = s.post(f"{BASE}/api/v1/database/", json={
        "database_name": DB_NAME, "sqlalchemy_uri": DB_URI, "expose_in_sqllab": True})
    resp.raise_for_status()
    db_id = resp.json()["id"]
    print(f"[superset] created database '{DB_NAME}' (id={db_id})")
    return db_id


def ensure_dataset(s, db_id: int, sql_path: str):
    name = re.sub(r"^\d+_", "", os.path.splitext(os.path.basename(sql_path))[0])
    with open(sql_path, encoding="utf-8") as f:
        sql = "\n".join(ln for ln in f if not ln.strip().startswith("--")).strip().rstrip(";")
    payload = {"database": db_id, "schema": "dw", "table_name": name, "sql": sql}
    resp = s.post(f"{BASE}/api/v1/dataset/", json=payload)
    if resp.status_code in (200, 201):
        print(f"[superset] created virtual dataset '{name}'")
    elif resp.status_code == 422 and "exist" in resp.text.lower():
        print(f"[superset] dataset '{name}' already exists — skipping")
    else:
        print(f"[superset] WARN dataset '{name}': {resp.status_code} {resp.text[:200]}")


def main():
    s = session()
    db_id = ensure_database(s)
    for sql_path in sorted(glob.glob(os.path.join(SQL_DIR, "*.sql"))):
        ensure_dataset(s, db_id, sql_path)
    print("[superset] done — now build the 3 dashboards (see README.md).")


if __name__ == "__main__":
    main()
