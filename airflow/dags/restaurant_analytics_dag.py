"""restaurant_analytics — end-to-end ELT orchestration.

Daily DAG that:
  1. extracts the OLTP source (Mongo/Postgres/CSV) into the bronze lake,
  2. transforms it with Spark into conformed dimensions and facts,
  3. runs the three analyses,
  4. loads the Postgres warehouse and refreshes the OLAP cubes,
  5. loads the Neo4j graph (co-purchase / referrals / routing mesh),
  6. reindexes Proyecto 1's ElasticSearch if the catalogue changed,
  7. runs data-quality assertions on the warehouse.

Spark jobs run via `spark-submit --master local[*]` inside the Airflow worker
(the image bundles a JVM + pyspark), so no separate Spark cluster is required;
point SPARK_MASTER at a standalone cluster to scale out without code changes.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

PROJECT = os.getenv("PROJECT_DIR", "/opt/project")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# Environment shared by every Spark job (lake mode => Hive catalog + JDBC).
JOB_ENV = (
    f"PYTHONPATH={PROJECT} "
    f"WAREHOUSE_MODE={os.getenv('WAREHOUSE_MODE', 'lake')} "
    f"LAKE_PATH={os.getenv('LAKE_PATH', '/lake/warehouse')} "
    f"SOURCE_KIND={os.getenv('SOURCE_KIND', 'csv')} "
    f"SOURCE_PATH={os.getenv('SOURCE_PATH', PROJECT + '/data/source')} "
    f"WAREHOUSE_JDBC_URL={os.getenv('WAREHOUSE_JDBC_URL', 'jdbc:postgresql://warehouse-db:5432/warehouse')} "
    f"WAREHOUSE_USER={os.getenv('WAREHOUSE_USER', 'warehouse')} "
    f"WAREHOUSE_PASSWORD={os.getenv('WAREHOUSE_PASSWORD', 'warehouse')} "
    f"HIVE_METASTORE_URI={os.getenv('HIVE_METASTORE_URI', 'thrift://hive-metastore:9083')} "
)


def spark_task(task_id: str, job: str, dag: DAG) -> BashOperator:
    submit = (
        f"{JOB_ENV} spark-submit --master {SPARK_MASTER} "
        f"--packages org.postgresql:postgresql:42.7.3 "
        f"{PROJECT}/spark/jobs/{job}"
    )
    return BashOperator(task_id=task_id, bash_command=submit, dag=dag)


def _load_neo4j():
    import runpy
    runpy.run_path(f"{PROJECT}/neo4j/load/load_graph.py", run_name="__main__")


def _reindex_elasticsearch():
    """Trigger Proyecto 1's /search/reindex when the catalogue changed.

    Best-effort: if Proyecto 1 isn't reachable we log and continue rather than
    failing the analytics pipeline (the two repos are independently deployable).
    """
    import urllib.request
    url = os.getenv("PROY1_REINDEX_URL")
    if not url:
        print("[reindex] PROY1_REINDEX_URL not set — skipping (Proyecto 1 not wired).")
        return
    try:
        req = urllib.request.Request(url, method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            print(f"[reindex] {url} -> {resp.status}")
    except Exception as exc:  # noqa: BLE001
        print(f"[reindex] WARN could not reach Proyecto 1 ({exc}); continuing.")


def _data_quality():
    """Assertions that fail the run if the warehouse is internally inconsistent."""
    import psycopg2
    url = os.getenv("WAREHOUSE_JDBC_URL", "jdbc:postgresql://warehouse-db:5432/warehouse")
    raw = url.replace("jdbc:postgresql://", "")
    hostport, db = raw.split("/", 1)
    host, port = (hostport.split(":") + ["5432"])[:2]
    conn = psycopg2.connect(host=host, port=port, dbname=db,
                            user=os.getenv("WAREHOUSE_USER", "warehouse"),
                            password=os.getenv("WAREHOUSE_PASSWORD", "warehouse"))
    checks = {
        "facts not empty":
            "SELECT count(*) FROM dw.fact_orders",
        "no orphan order_items (FK to product)":
            "SELECT count(*) FROM dw.fact_order_items fi "
            "LEFT JOIN dw.dim_product p USING(product_key) WHERE p.product_key IS NULL",
        "cube grand total == leaf sum (revenue)":
            "SELECT abs((SELECT revenue FROM dw.cube_revenue_time_category WHERE grp_flag=15) "
            "- (SELECT COALESCE(SUM(revenue),0) FROM dw.cube_revenue_time_category WHERE grp_flag=0))",
        "no negative net amounts":
            "SELECT count(*) FROM dw.fact_orders WHERE net_amount < 0",
    }
    failures = []
    with conn.cursor() as cur:
        for name, sql in checks.items():
            cur.execute(sql)
            val = cur.fetchone()[0]
            if name.startswith("facts not empty"):
                ok = val > 0
            elif name.startswith("cube grand total"):
                ok = float(val) < 0.01
            else:
                ok = val == 0
            print(f"[dq] {name}: {val} -> {'OK' if ok else 'FAIL'}")
            if not ok:
                failures.append(name)
    conn.close()
    if failures:
        raise ValueError(f"data-quality failures: {failures}")


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="restaurant_analytics",
    description="ELT: OLTP -> lakehouse -> warehouse/OLAP -> Neo4j -> dashboards",
    schedule="0 4 * * *",          # daily at 04:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["restaurantes", "spark", "olap", "neo4j"],
) as dag:

    start = EmptyOperator(task_id="start")
    extract = spark_task("extract", "extract.py", dag)
    dimensions = spark_task("build_dimensions", "build_dimensions.py", dag)
    facts = spark_task("build_facts", "build_facts.py", dag)
    analyses = spark_task("analyses", "analyses.py", dag)
    load_wh = spark_task("load_warehouse", "load_warehouse.py", dag)

    load_neo4j = PythonOperator(task_id="load_neo4j", python_callable=_load_neo4j)
    reindex_es = PythonOperator(task_id="reindex_elasticsearch",
                                python_callable=_reindex_elasticsearch)
    dq = PythonOperator(task_id="data_quality", python_callable=_data_quality)
    done = EmptyOperator(task_id="done")

    start >> extract >> dimensions >> facts >> analyses >> load_wh
    load_wh >> dq >> done
    facts >> load_neo4j >> done
    load_wh >> reindex_es >> done
