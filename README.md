# chameleon-airflow-e2e

Airflow DAGs for the Chameleon platform — the same CBS energy use case three ways.
git-synced by the operator's Airflow (`valueOverrides.airflow…gitSync`, `subPath: dags`);
`dbt/` is a submodule of [`chameleon-cbs-dlt-dbt`](https://github.com/sovereign-data/chameleon-cbs-dlt-dbt).

| DAG | Ingest | Transform | Shows |
|---|---|---|---|
| `cbs_dbt_in_airflow` (A) | dlt → `s3://cbs-landing` | **cosmos**: dbt-trino in the Airflow worker, one task per model/test | dbt 100% in Airflow |
| `cbs_dbt_in_chameleon` (B) | dlt → `s3://cbs-landing` | `ChameleonDbtRunOperator` run + test in the backend's dbt-runner | dbt in Chameleon, orchestrated by Airflow |
| `cbs_upload_e2e` | CSV → BFF `/upload` → Iceberg | `ChameleonSqlSensor` + `ChameleonSqlOperator` (CTAS) | no dlt, no S3 creds in Airflow |

A and B produce the same `cbs_gold.energy_cost_per_dwelling`; compare run time,
logs, lineage (Chameleon UI only for B) and who holds credentials.

## Setup

1. Platform: `spec.airflow: true` + gitSync + `sqe.extraTvfPrefixes` (see `chameleon-platform-tf` README).
2. `chameleon-platform-tf`: `terraform apply`, then add `chameleon_default` + Variables `cbs_catalog`, `cbs_dbt_project_id`.
3. Connection `cbs_landing_s3` (type aws): RustFS access key with write on `cbs-landing`,
   extra `{"endpoint_url": "http://rustfs-svc.rustfs:9000"}`.
4. Trigger `cbs_dbt_in_chameleon` and `cbs_dbt_in_airflow`.

## Local check

```bash
uv sync --group dev
uv pip install -e ../apache-airflow-providers-chameleon   # provider isn't on PyPI
# cosmos DAG needs a dbt-trino venv; repo 2's works:
ELT_PYTHON=../chameleon-cbs-dlt-dbt/.venv/bin/python AIRFLOW_HOME=$PWD/.airflow uv run pytest -q tests
```
