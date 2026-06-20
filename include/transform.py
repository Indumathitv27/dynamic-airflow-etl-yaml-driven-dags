"""
Pure, dependency-free ETL logic extracted from the Airflow DAG.

These functions contain the real transformation and data-quality rules.
They take plain Python inputs and return plain Python outputs, so they can
be unit-tested without a database, Airflow, or network access.

The DAG's PythonOperator callables (fetch / load / publish) handle I/O and
call into these functions for the logic that actually matters.
"""

from datetime import datetime


def parse_hourly(payload, pipeline_name, fetched_at):
    """Turn an Open-Meteo 'hourly' payload into staging rows.

    Returns a list of tuples:
        (pipeline_name, ts, temperature, windspeed, fetched_at)

    Mirrors the staging-load logic from transform_to_staging:
    the row count is bounded by the SHORTEST of the three parallel
    arrays so a ragged API response can never produce misaligned rows.
    """
    hourly = (payload or {}).get("hourly", {}) or {}
    times = hourly.get("time", []) or []
    temps = hourly.get("temperature_2m", []) or []
    winds = hourly.get("windspeed_10m", []) or []

    n = min(len(times), len(temps), len(winds))

    rows = []
    for i in range(n):
        rows.append((pipeline_name, times[i], temps[i], winds[i], fetched_at))
    return rows


def evaluate_dq(row_count, null_temps):
    """Data-quality gate for a staging load.

    Passes only when at least one row was loaded AND not every row has a
    null temperature. Returns True on pass, False on fail. This is the
    exact rule enforced in dq_checks, lifted out so it can be tested.
    """
    return (row_count > 0) and (null_temps < row_count)