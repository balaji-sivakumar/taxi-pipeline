# ruff: noqa: DTZ001 -- naive datetimes here deliberately mirror quality.py's
# own naive comparisons against TLC's tz-naive timestamp columns.
from datetime import datetime

import polars as pl

from taxi_pipeline.quality import apply_quality_rules, rejected_path_for, write_rejected

BASE_COLUMNS = {
    "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 0),
    "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 20),
    "trip_distance": 5.0,
    "fare_amount": 10.0,
    "total_amount": 12.0,
    "passenger_count": 2,
}


def _row(**overrides):
    row = dict(BASE_COLUMNS)
    row.update(overrides)
    return row


def test_clean_row_is_valid():
    lf = pl.LazyFrame([_row()])
    valid_df, rejected_df = apply_quality_rules(lf, 2024, 1)

    assert valid_df.height == 1
    assert rejected_df.height == 0


def test_null_passenger_count_is_valid_not_rejected():
    lf = pl.LazyFrame([_row(passenger_count=None)])
    valid_df, rejected_df = apply_quality_rules(lf, 2024, 1)

    assert valid_df.height == 1
    assert rejected_df.height == 0


def test_zero_passenger_count_is_rejected():
    lf = pl.LazyFrame([_row(passenger_count=0)])
    _, rejected_df = apply_quality_rules(lf, 2024, 1)

    assert rejected_df["failed_rules"].to_list() == [["passenger_count_plausible"]]


def test_impossible_trip_distance_is_rejected():
    lf = pl.LazyFrame([_row(trip_distance=500.0)])
    _, rejected_df = apply_quality_rules(lf, 2024, 1)

    assert rejected_df["failed_rules"].to_list() == [["trip_distance_plausible"]]


def test_pickup_outside_file_month_is_rejected():
    lf = pl.LazyFrame([_row(tpep_pickup_datetime=datetime(2023, 12, 31, 23, 0))])
    _, rejected_df = apply_quality_rules(lf, 2024, 1)

    assert rejected_df["failed_rules"].to_list() == [["pickup_within_file_month"]]


def test_dropoff_before_pickup_is_rejected():
    lf = pl.LazyFrame(
        [
            _row(
                tpep_pickup_datetime=datetime(2024, 1, 15, 10, 0),
                tpep_dropoff_datetime=datetime(2024, 1, 15, 9, 0),
            )
        ]
    )
    _, rejected_df = apply_quality_rules(lf, 2024, 1)

    assert rejected_df["failed_rules"].to_list() == [["dropoff_not_before_pickup"]]


def test_row_can_fail_multiple_rules_at_once():
    lf = pl.LazyFrame([_row(fare_amount=-10.0, total_amount=-12.0)])
    _, rejected_df = apply_quality_rules(lf, 2024, 1)

    failed = set(rejected_df["failed_rules"].to_list()[0])
    assert failed == {"fare_amount_non_negative", "total_amount_non_negative"}


def test_write_rejected_persists_to_expected_partitioned_path(tmp_path):
    lf = pl.LazyFrame([_row(trip_distance=500.0)])
    _, rejected_df = apply_quality_rules(lf, 2024, 1)

    written_path = write_rejected(rejected_df, 2024, 1, rejected_dir=tmp_path)

    assert written_path == rejected_path_for(2024, 1, tmp_path)
    assert written_path.exists()
    assert pl.read_parquet(written_path).height == 1
