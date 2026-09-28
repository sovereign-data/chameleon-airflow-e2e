"""Shared bits of the CBS energy DAGs.

Connections (Vault secrets backend or AIRFLOW_CONN_* env):
  chameleon_default  type chameleon, auth_type=service_principal: the
                     workspace service principal (Terraform output).
  cbs_landing_s3     type aws: RustFS key for bucket cbs-landing,
                     extra {"endpoint_url": "http://rustfs-svc.rustfs:9000"}.
Variables:
  cbs_catalog            workspace catalog, default ws_cbs_energy
  cbs_dbt_project_id     chameleon_project id (Terraform output)
"""

import os
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator

CONN = "chameleon_default"
CATALOG = "{{ var.value.get('cbs_catalog', 'ws_cbs_energy') }}"

# git-sync layout: <repo>/dags (dags_folder) and <repo>/dbt (submodule).
DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"
ELT_PYTHON = os.environ.get("ELT_PYTHON", "/opt/elt/bin/python")  # dlt + dbt-trino venv baked into the image


def land_cbs() -> BashOperator:
    """dlt: CBS OData -> s3://cbs-landing/raw/cbs/<table>/ (replace)."""
    return BashOperator(
        task_id="land_cbs",
        bash_command=f"{ELT_PYTHON} {DBT_DIR}/ingest/cbs_pipeline.py",
        env={
            "DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID": "{{ conn.cbs_landing_s3.login }}",
            "DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY": "{{ conn.cbs_landing_s3.password }}",
            "DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL": "{{ conn.cbs_landing_s3.extra_dejson.endpoint_url }}",
        },
        append_env=True,
    )


def sqe_token_env(context) -> None:
    """on_execute_callback: put a fresh SQE token in this task's env.

    KubernetesExecutor runs each task in its own pod, so the process env is
    private to the task; the token never lands in XCom or rendered fields.
    """
    from airflow.providers.chameleon.hooks.chameleon import ChameleonHook

    os.environ["DBT_ACCESS_TOKEN"] = ChameleonHook(CONN)._token()
