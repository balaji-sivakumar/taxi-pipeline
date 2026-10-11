"""Integration tests: real stage functions composed together, not mocked.

Unlike test_pipeline.py (which mocks every stage to test process_month's own
control flow in isolation), these tests exercise the actual quality rules,
transform, aggregation, and curated write together against a small synthetic
raw file -- the thing no unit test can catch is a composition/contract bug
between two real stages.
"""

# ruff: noqa: DTZ001 -- naive datetimes here deliberately mirror quality.py's
# own naive comparisons against TLC's tz-naive timestamp columns.
from datetime import datetime
from unittest.mock import patch

import polars as pl

from taxi_pipeline.manifest import load_manifest
from taxi_pipeline.pipeline import process_month, process_months
from taxi_pipeline.schema import YELLOW_TRIPDATA_SCHEMA


def _trip_row(**overrides):
    row = {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 20),
        "passenger_count": 1,
        "trip_distance": 2.0,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": 10,
        "DOLocationID": 20,
        "payment_type": 1,
        "fare_amount": 10.0,
        "extra": 0.0,
        "mta_tax": 0.5,
        "tip_amount": 1.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.3,
        "total_amount": 11.8,
        "congestion_surcharge": 2.5,
        "Airport_fee": 0.0,
    }
    row.update(overrides)
    return row


def _write_raw_file(path, rows):
    pl.DataFrame(rows, schema=YELLOW_TRIPDATA_SCHEMA).write_parquet(path)


def test_process_month_runs_real_pipeline_end_to_end(tmp_path):
    raw_path = tmp_path / "raw.parquet"
    _write_raw_file(
        raw_path,
        [
            _trip_row(),
            _trip_row(
                tpep_pickup_datetime=datetime(2024, 1, 15, 14, 0),
                tpep_dropoff_datetime=datetime(2024, 1, 15, 14, 10),
                PULocationID=30,
                DOLocationID=40,
                passenger_count=2,
            ),
            _trip_row(fare_amount=-5.0, total_amount=-4.2),  # should be quarantined
        ],
    )

    with patch("taxi_pipeline.pipeline.download_month", return_value=raw_path):
        result = process_month(
            2024,
            1,
            manifest_path=tmp_path / "manifest.json",
            raw_dir=tmp_path,
            rejected_dir=tmp_path / "rejected",
            curated_dir=tmp_path / "curated",
        )

    assert result["valid_rows"] == 2
    assert result["rejected_rows"] == 1
    assert result["curated_rows"] > 0

    rejected = pl.read_parquet(
        tmp_path
        / "rejected"
        / "year=2024"
        / "month=01"
        / "yellow_tripdata_2024-01_rejected.parquet"
    )
    assert rejected["failed_rules"].to_list() == [
        ["fare_amount_non_negative", "total_amount_non_negative"]
    ]

    curated = pl.read_parquet(
        tmp_path
        / "curated"
        / "year=2024"
        / "month=01"
        / "hourly_demand_2024-01.parquet"
    )
    busy_hour = curated.filter(
        (pl.col("pickup_hour") == datetime(2024, 1, 15, 10, 0))
        & (pl.col("pickup_location_id") == 10)
    )
    assert busy_hour["pickup_count"].item() == 1


def test_process_month_raises_clear_error_on_schema_drift(tmp_path):
    raw_path = tmp_path / "raw.parquet"
    row = _trip_row()
    del row["trip_distance"]  # simulate a source column being renamed/removed
    pl.DataFrame([row]).write_parquet(raw_path)

    with patch("taxi_pipeline.pipeline.download_month", return_value=raw_path):
        try:
            process_month(
                2024,
                1,
                manifest_path=tmp_path / "manifest.json",
                raw_dir=tmp_path,
                rejected_dir=tmp_path / "rejected",
                curated_dir=tmp_path / "curated",
            )
            raised = False
        except RuntimeError as exc:
            raised = True
            message = str(exc)

    assert raised
    assert "trip_distance" in message


def test_process_months_runs_multiple_real_months_independently(tmp_path):
    jan_path = tmp_path / "jan.parquet"
    feb_path = tmp_path / "feb.parquet"
    _write_raw_file(jan_path, [_trip_row()])
    _write_raw_file(
        feb_path,
        [
            _trip_row(
                tpep_pickup_datetime=datetime(2024, 2, 10, 9, 0),
                tpep_dropoff_datetime=datetime(2024, 2, 10, 9, 15),
                PULocationID=99,
                DOLocationID=88,
            ),
            _trip_row(
                tpep_pickup_datetime=datetime(2024, 2, 10, 9, 0),
                tpep_dropoff_datetime=datetime(2024, 2, 10, 9, 15),
                PULocationID=99,
                DOLocationID=88,
            ),
        ],
    )

    def fake_download(year, month, raw_dir=None):
        return jan_path if month == 1 else feb_path

    manifest_path = tmp_path / "manifest.json"
    with patch("taxi_pipeline.pipeline.download_month", side_effect=fake_download):
        results = process_months(
            [(2024, 1), (2024, 2)],
            manifest_path=manifest_path,
            rejected_dir=tmp_path / "rejected",
            curated_dir=tmp_path / "curated",
        )

    assert results[0]["valid_rows"] == 1
    assert results[1]["valid_rows"] == 2

    jan_curated = pl.read_parquet(
        tmp_path
        / "curated"
        / "year=2024"
        / "month=01"
        / "hourly_demand_2024-01.parquet"
    )
    feb_curated = pl.read_parquet(
        tmp_path
        / "curated"
        / "year=2024"
        / "month=02"
        / "hourly_demand_2024-02.parquet"
    )

    # each month's curated output must reflect only its own data -- no cross-contamination
    assert (
        jan_curated.filter(pl.col("pickup_location_id") == 10)["pickup_count"].sum()
        == 1
    )
    assert (
        jan_curated.filter(pl.col("pickup_location_id") == 99)["pickup_count"].sum()
        == 0
    )
    assert (
        feb_curated.filter(pl.col("pickup_location_id") == 99)["pickup_count"].sum()
        == 2
    )

    assert set(load_manifest(manifest_path).keys()) == {"2024-01", "2024-02"}


def test_process_months_preserves_completed_real_months_when_a_later_one_fails(
    tmp_path,
):
    jan_path = tmp_path / "jan.parquet"
    mar_path = tmp_path / "mar.parquet"
    _write_raw_file(jan_path, [_trip_row()])
    _write_raw_file(
        mar_path,
        [
            _trip_row(
                tpep_pickup_datetime=datetime(2024, 3, 1, 0, 0),
                tpep_dropoff_datetime=datetime(2024, 3, 1, 0, 10),
            )
        ],
    )

    def fake_download(year, month, raw_dir=None):
        if month == 2:
            raise RuntimeError("simulated real download failure")
        return jan_path if month == 1 else mar_path

    manifest_path = tmp_path / "manifest.json"
    curated_dir = tmp_path / "curated"
    with patch("taxi_pipeline.pipeline.download_month", side_effect=fake_download):
        results = process_months(
            [(2024, 1), (2024, 2), (2024, 3)],
            manifest_path=manifest_path,
            rejected_dir=tmp_path / "rejected",
            curated_dir=curated_dir,
        )

    assert results[0]["valid_rows"] == 1
    assert "error" in results[1]
    assert results[2]["valid_rows"] == 1

    # the failed month leaves no trace: no manifest entry, no curated directory at all
    assert set(load_manifest(manifest_path).keys()) == {"2024-01", "2024-03"}
    assert (curated_dir / "year=2024" / "month=01").exists()
    assert (curated_dir / "year=2024" / "month=03").exists()
    assert not (curated_dir / "year=2024" / "month=02").exists()
