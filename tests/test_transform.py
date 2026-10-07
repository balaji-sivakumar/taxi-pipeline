# ruff: noqa: DTZ001 -- naive datetimes here deliberately mirror quality.py's
# own naive comparisons against TLC's tz-naive timestamp columns.
from datetime import datetime

import polars as pl

from taxi_pipeline.transform import clean_trips


def _row(**overrides):
    row = {
        "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 30),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 45),
        "PULocationID": 100,
        "DOLocationID": 200,
        "trip_distance": 3.5,
        "passenger_count": 2,
        "fare_amount": 15.0,
        "total_amount": 18.0,
    }
    row.update(overrides)
    return row


def test_clean_trips_renames_columns_to_curated_names():
    df = clean_trips(pl.DataFrame([_row()]))

    assert set(df.columns) == {
        "pickup_datetime",
        "pickup_hour",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "passenger_count",
        "fare_amount",
        "total_amount",
    }


def test_clean_trips_preserves_row_count():
    df = clean_trips(pl.DataFrame([_row(), _row(PULocationID=50)]))

    assert df.height == 2


def test_pickup_hour_buckets_to_the_containing_hour():
    df = clean_trips(
        pl.DataFrame(
            [
                _row(tpep_pickup_datetime=datetime(2024, 1, 15, 10, 0, 0)),
                _row(tpep_pickup_datetime=datetime(2024, 1, 15, 10, 59, 59)),
                _row(tpep_pickup_datetime=datetime(2024, 1, 15, 11, 0, 0)),
            ]
        )
    )

    assert df["pickup_hour"].to_list() == [
        datetime(2024, 1, 15, 10, 0, 0),
        datetime(2024, 1, 15, 10, 0, 0),
        datetime(2024, 1, 15, 11, 0, 0),
    ]


def test_pickup_hour_buckets_correctly_across_a_day_boundary():
    df = clean_trips(
        pl.DataFrame([_row(tpep_pickup_datetime=datetime(2024, 1, 15, 23, 59, 59))])
    )

    assert df["pickup_hour"].to_list() == [datetime(2024, 1, 15, 23, 0, 0)]
