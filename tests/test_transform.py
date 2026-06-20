"""
Unit tests for the pure ETL logic in include/transform.py.

These assert actual behavior:
  - the transform aligns and truncates ragged API arrays correctly
  - empty / missing payloads degrade safely to zero rows
  - the data-quality gate flags the cases it is meant to catch
"""

from datetime import datetime

import pytest

from include.transform import parse_hourly, evaluate_dq


FETCHED = datetime(2025, 1, 1, 0, 0, 0)


def _payload(times, temps, winds):
    return {
        "hourly": {
            "time": times,
            "temperature_2m": temps,
            "windspeed_10m": winds,
        }
    }


# ---------- parse_hourly ----------

def test_parse_hourly_happy_path():
    payload = _payload(
        ["2025-01-01T00:00", "2025-01-01T01:00"],
        [5.0, 6.0],
        [10.0, 11.0],
    )
    rows = parse_hourly(payload, "weather_newyork", FETCHED)

    assert len(rows) == 2
    assert rows[0] == ("weather_newyork", "2025-01-01T00:00", 5.0, 10.0, FETCHED)
    assert rows[1] == ("weather_newyork", "2025-01-01T01:00", 6.0, 11.0, FETCHED)


def test_parse_hourly_truncates_to_shortest_array():
    # 3 timestamps but only 2 temps and 2 winds -> must yield 2 rows, not 3
    payload = _payload(
        ["t0", "t1", "t2"],
        [1.0, 2.0],
        [9.0, 9.5],
    )
    rows = parse_hourly(payload, "p", FETCHED)

    assert len(rows) == 2
    # never reaches the unmatched third timestamp
    assert all(r[1] in ("t0", "t1") for r in rows)


def test_parse_hourly_empty_hourly_block():
    rows = parse_hourly({"hourly": {}}, "p", FETCHED)
    assert rows == []


def test_parse_hourly_missing_hourly_key():
    rows = parse_hourly({}, "p", FETCHED)
    assert rows == []


def test_parse_hourly_none_payload():
    rows = parse_hourly(None, "p", FETCHED)
    assert rows == []


def test_parse_hourly_carries_pipeline_and_fetched_at():
    payload = _payload(["t0"], [3.3], [4.4])
    rows = parse_hourly(payload, "weather_texas_city", FETCHED)
    assert rows[0][0] == "weather_texas_city"
    assert rows[0][4] == FETCHED


# ---------- evaluate_dq ----------

def test_dq_passes_on_clean_load():
    assert evaluate_dq(row_count=96, null_temps=0) is True


def test_dq_fails_on_zero_rows():
    assert evaluate_dq(row_count=0, null_temps=0) is False


def test_dq_fails_when_all_temps_null():
    assert evaluate_dq(row_count=10, null_temps=10) is False


def test_dq_passes_with_some_nulls_but_not_all():
    assert evaluate_dq(row_count=10, null_temps=3) is True


@pytest.mark.parametrize(
    "row_count,null_temps,expected",
    [
        (1, 0, True),
        (1, 1, False),
        (0, 0, False),
        (5, 4, True),
        (5, 5, False),
    ],
)
def test_dq_boundaries(row_count, null_temps, expected):
    assert evaluate_dq(row_count, null_temps) is expected