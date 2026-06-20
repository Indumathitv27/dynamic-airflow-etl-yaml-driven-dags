import json
from datetime import datetime, timedelta
from include.transform import parse_hourly, evaluate_dq

import requests
import yaml
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


WAREHOUSE_CONN = {
    "host": "warehouse",
    "port": 5432,
    "dbname": "analytics",
    "user": "warehouse",
    "password": "warehouse",
}

CONFIG_PATH = "/opt/airflow/include/config/pipelines.yml"
CREATE_TABLES_SQL_PATH = "/opt/airflow/include/sql/create_tables.sql"


def get_conn():
    return psycopg2.connect(**WAREHOUSE_CONN)


def init_tables():
    with open(CREATE_TABLES_SQL_PATH, "r") as f:
        sql = f.read()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def fetch_weather(pipeline_name: str, latitude: float, longitude: float, timezone: str, **context):
    fetched_at = datetime.utcnow()

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,windspeed_10m",
        "timezone": timezone,
        "past_days": 1,
        "forecast_days": 1,
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO raw_weather (pipeline_name, fetched_at, payload) VALUES (%s, %s, %s)",
                (pipeline_name, fetched_at, json.dumps(payload)),
            )
        conn.commit()
    finally:
        conn.close()

    context["ti"].xcom_push(key="fetched_at", value=fetched_at.isoformat())


def transform_to_staging(pipeline_name: str, **context):
    fetched_at_iso = context["ti"].xcom_pull(key="fetched_at")
    fetched_at = datetime.fromisoformat(fetched_at_iso)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload
                FROM raw_weather
                WHERE pipeline_name = %s AND fetched_at = %s
                """,
                (pipeline_name, fetched_at),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("No raw payload found for this run.")

            payload = row[0]
            rows = parse_hourly(payload, pipeline_name, fetched_at)

            for r in rows:
                cur.execute(
                    """
                    INSERT INTO stg_weather_hourly (pipeline_name, ts, temperature, windspeed, fetched_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    r,
                )
            inserted = len(rows)
        conn.commit()
    finally:
        conn.close()

    context["ti"].xcom_push(key="records_loaded", value=inserted)


def dq_checks(pipeline_name: str, **context):
    fetched_at_iso = context["ti"].xcom_pull(key="fetched_at")
    fetched_at = datetime.fromisoformat(fetched_at_iso)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM stg_weather_hourly
                WHERE pipeline_name = %s AND fetched_at = %s
                """,
                (pipeline_name, fetched_at),
            )
            row_count = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM stg_weather_hourly
                WHERE pipeline_name = %s AND fetched_at = %s AND temperature IS NULL
                """,
                (pipeline_name, fetched_at),
            )
            null_temps = cur.fetchone()[0]
    finally:
        conn.close()

    passed = evaluate_dq(row_count, null_temps)
    if not passed:
        raise ValueError(f"DQ failed: row_count={row_count}, null_temps={null_temps}")


def publish_metrics(pipeline_name: str, **context):
    dag_run_id = context["run_id"]
    start_time = context["ti"].start_date
    duration_seconds = int((datetime.utcnow() - start_time.replace(tzinfo=None)).total_seconds())

    records_loaded = context["ti"].xcom_pull(key="records_loaded") or 0

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_run_metrics
                (pipeline_name, dag_run_id, records_raw, records_loaded, dq_passed, duration_seconds)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (pipeline_name, dag_run_id, records_loaded, records_loaded, True, duration_seconds),
            )
        conn.commit()
    finally:
        conn.close()


def create_dag(pipeline):
    pipeline_name = pipeline["name"]
    schedule = pipeline["schedule"]

    default_args = {
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }

    dag = DAG(
        dag_id=f"{pipeline_name}_dag",
        default_args=default_args,
        start_date=datetime(2025, 1, 1),
        schedule=schedule,
        catchup=False,
        tags=["dynamic", "weather", "etl"],
    )

    with dag:
        t0 = PythonOperator(task_id="init_tables", python_callable=init_tables)

        extract = PythonOperator(
            task_id="extract_api",
            python_callable=fetch_weather,
            op_kwargs={
                "pipeline_name": pipeline_name,
                "latitude": pipeline["latitude"],
                "longitude": pipeline["longitude"],
                "timezone": pipeline["timezone"],
            },
        )

        transform = PythonOperator(
            task_id="transform_to_staging",
            python_callable=transform_to_staging,
            op_kwargs={"pipeline_name": pipeline_name},
        )

        dq = PythonOperator(
            task_id="dq_checks",
            python_callable=dq_checks,
            op_kwargs={"pipeline_name": pipeline_name},
        )

        metrics = PythonOperator(
            task_id="publish_metrics",
            python_callable=publish_metrics,
            op_kwargs={"pipeline_name": pipeline_name},
        )

        done = EmptyOperator(task_id="done")

        t0 >> extract >> transform >> dq >> metrics >> done

    return dag


with open(CONFIG_PATH, "r") as f:
    cfg = yaml.safe_load(f)

for p in cfg["pipelines"]:
    globals()[f"{p['name']}_dag"] = create_dag(p)