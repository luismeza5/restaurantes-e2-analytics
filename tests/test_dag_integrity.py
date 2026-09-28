"""DAG integrity — the DAG must import with no errors and have the expected wiring.

Skipped if Airflow isn't installed (e.g. a minimal local env); CI installs it.
"""
import os
import sys

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow-test")


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=os.path.join(ROOT, "airflow", "dags"),
                  include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_dag_present_and_scheduled(dagbag):
    dag = dagbag.dags["restaurant_analytics"]
    assert dag is not None
    assert dag.schedule_interval == "0 4 * * *"


def test_expected_tasks_exist(dagbag):
    dag = dagbag.dags["restaurant_analytics"]
    expected = {"start", "extract", "build_dimensions", "build_facts", "analyses",
                "load_warehouse", "load_neo4j", "reindex_elasticsearch",
                "data_quality", "done"}
    assert expected == set(dag.task_ids)


def test_pipeline_order(dagbag):
    dag = dagbag.dags["restaurant_analytics"]
    facts = dag.get_task("build_facts")
    load_wh = dag.get_task("load_warehouse")
    assert "build_dimensions" in facts.upstream_task_ids
    assert "analyses" in load_wh.upstream_task_ids
    assert "load_warehouse" in dag.get_task("data_quality").upstream_task_ids
