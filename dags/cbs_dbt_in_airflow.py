"""Method A — dbt 100% in Airflow.

dlt lands CBS parquet, then cosmos renders the dbt project (git submodule) as
one Airflow task per model/test and runs dbt-trino in the worker against SQE.
Airflow owns scheduling, retries, logs and the dbt graph; the platform only
sees SQL from a service principal.
"""

import pendulum
from airflow.sdk import DAG
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, LoadMode

from cbs_common import DBT_DIR, ELT_PYTHON, land_cbs, sqe_token_env

DBT = ELT_PYTHON.replace("python", "dbt")

with DAG(
    dag_id="cbs_dbt_in_airflow",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Amsterdam"),
    schedule=None,  # CBS updates monthly; trigger manually for the demo
    catchup=False,
    tags=["cbs", "dlt", "dbt", "method-a"],
    doc_md=__doc__,
):
    dbt = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(DBT_DIR),
        profile_config=ProfileConfig(
            profile_name="trino", target_name="dev", profiles_yml_filepath=DBT_DIR / "profiles.yml"
        ),
        # Subprocess into the elt venv: dbt must not share Airflow's Python env.
        execution_config=ExecutionConfig(dbt_executable_path=DBT, invocation_mode=InvocationMode.SUBPROCESS),
        # dbt ls at parse time never connects (profile token defaults to "").
        render_config=RenderConfig(load_method=LoadMode.DBT_LS, dbt_executable_path=DBT,
                                   invocation_mode=InvocationMode.SUBPROCESS),
        # dbt-trino forces HTTPS for JWT auth, so SQE is reached over its TLS
        # route (profile default sql.<domain>:443), not plain-HTTP sqe.sqe:8080.
        # `env` is templated and reaches the dbt subprocess; the token comes
        # from the callback.
        operator_args={
            "append_env": True,
            "on_execute_callback": sqe_token_env,
            "env": {"DBT_TARGET_CATALOG": "{{ var.value.get('cbs_catalog', 'ws_cbs_energy') }}"},
        },
    )

    land_cbs() >> dbt
