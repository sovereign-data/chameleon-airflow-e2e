"""Other ingestion path — no dlt, no S3 credentials in Airflow.

A plain task pulls CBS tariffs as CSV and pushes the file through the
Chameleon BFF upload endpoint, which stages it and loads it into an Iceberg
table. Then provider sensors/operators take over: wait for rows, build a
yearly summary with SQL, and read it back.
"""

import csv
import io

import httpx
import pendulum
from airflow.providers.chameleon.hooks.chameleon import ChameleonHook
from airflow.providers.chameleon.operators.chameleon_sql import ChameleonSqlOperator
from airflow.providers.chameleon.sensors.chameleon_sql import ChameleonSqlSensor
from airflow.sdk import DAG, task

from cbs_common import CATALOG, CONN

FEED = "https://opendata.cbs.nl/ODataFeed/odata/85592NED/TypedDataSet"
NS, TABLE = "cbs_raw", "energy_tariffs_upload"


@task
def upload_tariffs(catalog: str) -> dict:
    rows, url, params = [], FEED, {"$format": "json"}
    while url:  # OData feed paging
        page = httpx.get(url, params=params, timeout=60).raise_for_status().json()
        rows += page["value"]
        url, params = page.get("odata.nextLink"), None
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["period", "vat", "elec_eur_kwh", "gas_eur_m3"])
    for r in rows:
        writer.writerow([r["Perioden"].strip(), r["Btw"].strip(),
                         r["VariabelLeveringstariefContractprijs_9"], r["VariabelLeveringstariefContractprijs_3"]])

    hook = ChameleonHook(CONN)
    resp = httpx.post(
        f"{hook._base_url()}/catalogs/{catalog}/namespaces/{NS}/tables/{TABLE}/upload",
        headers=hook._headers(),
        files={"file": ("tariffs.csv", buf.getvalue().encode(), "text/csv")},
        data={"format": "csv", "mode": "overwrite", "create_if_missing": "true"},
        verify=hook._verify(),
        timeout=300,
    ).raise_for_status().json()
    return {"rows_written": resp["rows_written"], "snapshot_id": resp["snapshot_id"]}


with DAG(
    dag_id="cbs_upload_e2e",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Amsterdam"),
    schedule="@monthly",
    catchup=False,
    tags=["cbs", "upload", "chameleon"],
    doc_md=__doc__,
    default_args={"chameleon_conn_id": CONN},
):
    loaded = upload_tariffs(CATALOG)
    landed = ChameleonSqlSensor(
        task_id="rows_visible",
        sql=f"select count(*) > 0 from {CATALOG}.{NS}.{TABLE}",
        poke_interval=30,
        timeout=600,
    )
    summary = ChameleonSqlOperator(
        task_id="yearly_summary",
        sql=f"""
            create or replace table {CATALOG}.{NS}.energy_tariffs_yearly as
            select substr(period, 1, 4) as year,
                   avg(elec_eur_kwh) as elec_eur_kwh,
                   avg(gas_eur_m3) as gas_eur_m3
            from {CATALOG}.{NS}.{TABLE}
            where vat = 'A048944'  -- incl. VAT
              and substr(period, 5, 2) = 'MM'
            group by 1
        """,
    )
    readback = ChameleonSqlOperator(
        task_id="read_summary",
        sql=f"select * from {CATALOG}.{NS}.energy_tariffs_yearly order by year",
    )

    loaded >> landed >> summary >> readback
