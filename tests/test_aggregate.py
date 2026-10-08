# ruff: noqa: DTZ001 -- naive datetimes here deliberately mirror quality.py's
# own naive comparisons against TLC's tz-naive timestamp columns.
from datetime import datetime

import polars as pl

from taxi_pipeline.aggregate import aggregate_hourly_demand

HOURS_IN_JANUARY_2024 = 31 * 24


def test_grid_has_one_row_per_hour_per_observed_zone():
    cleaned = pl.DataFrame(
        {
            "pickup_hour": [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 0, 0)],
            "pickup_location_id": [10, 20],
        }
    )

    demand = aggregate_hourly_demand(cleaned, 2024, 1)

    assert demand.height == HOURS_IN_JANUARY_2024 * 2


def test_pickup_count_sums_to_input_row_count():
    cleaned = pl.DataFrame(
        {
            "pickup_hour": [
                datetime(2024, 1, 1, 0, 0),
                datetime(2024, 1, 1, 0, 0),
                datetime(2024, 1, 1, 5, 0),
            ],
            "pickup_location_id": [10, 10, 10],
        }
    )

    demand = aggregate_hourly_demand(cleaned, 2024, 1)

    assert demand["pickup_count"].sum() == cleaned.height


def test_hour_with_no_trips_is_explicit_zero_not_missing():
    cleaned = pl.DataFrame(
        {
            "pickup_hour": [datetime(2024, 1, 1, 0, 0)],
            "pickup_location_id": [10],
        }
    )

    demand = aggregate_hourly_demand(cleaned, 2024, 1)

    quiet_hour = demand.filter(
        (pl.col("pickup_hour") == datetime(2024, 1, 1, 5, 0))
        & (pl.col("pickup_location_id") == 10)
    )
    assert quiet_hour.height == 1
    assert quiet_hour["pickup_count"].item() == 0
