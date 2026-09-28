"""Method B — dbt run by Chameleon, orchestrated by Airflow.

Same dlt landing, then the backend runs the same dbt project (registered by
Terraform as a chameleon_project from the GitHub repo) in its dbt-runner pod.
Airflow only triggers and waits (deferrable); lineage, run history and
access control stay in the platform.
"""

import pendulum
from airflow.providers.chameleon.operators.chameleon import ChameleonDbtRunOperator
from airflow.providers.chameleon.operators.chameleon_sql import ChameleonSqlOperator
from airflow.sdk import DAG

from cbs_common import CATALOG, CONN, land_cbs

PROJECT = "{{ var.value.cbs_dbt_project_id }}"

with DAG(
    dag_id="cbs_dbt_in_chameleon",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Amsterdam"),
    schedule=None,
    catchup=False,
    tags=["cbs", "dlt", "dbt", "chameleon", "method-b"],
    doc_md=__doc__,
    default_args={"chameleon_conn_id": CONN, "deferrable": True},
):
    run = ChameleonDbtRunOperator(task_id="dbt_run", project_id=PROJECT, action="run")
    test = ChameleonDbtRunOperator(task_id="dbt_test", project_id=PROJECT, action="test")
    check = ChameleonSqlOperator(
        task_id="gold_has_rows",
        sql=f"select count(*) as n, max(year) as latest_year from {CATALOG}.cbs_gold.energy_cost_per_dwelling",
        deferrable=False,
    )

    land_cbs() >> run >> test >> check
