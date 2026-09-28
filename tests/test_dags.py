"""Every DAG imports cleanly. The cosmos DAG runs `dbt ls`, so it needs a
dbt-trino venv: ELT_PYTHON=<venv>/bin/python (the image has /opt/elt)."""

from pathlib import Path

import os

import pytest

# cosmos caches dbt ls in Airflow Variables; no metadata DB in a unit test.
os.environ.setdefault("AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS", "False")

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.mark.parametrize("name", ["cbs_dbt_in_chameleon", "cbs_upload_e2e", "cbs_dbt_in_airflow"])
def test_dag_imports(name):
    pytest.importorskip("airflow.providers.chameleon")
    from airflow.dag_processing.dagbag import DagBag

    if name == "cbs_dbt_in_airflow" and not Path(os.environ.get("ELT_PYTHON", "/opt/elt/bin/python")).with_name("dbt").exists():
        pytest.skip("cosmos needs the image's dbt")
    bag = DagBag(dag_folder=str(DAGS / f"{name}.py"))
    assert not bag.import_errors, bag.import_errors
    assert name in bag.dag_ids
