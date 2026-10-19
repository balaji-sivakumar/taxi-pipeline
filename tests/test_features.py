# ruff: noqa: DTZ001 -- naive datetimes here deliberately mirror quality.py's
# own naive comparisons against TLC's tz-naive timestamp columns.
from datetime import datetime

import polars as pl

from taxi_pipeline.features import add_calendar_features, add_lag_feature


def test_add_calendar_features_extracts_correct_components():
    df = pl.DataFrame({"pickup_hour": [datetime(2024, 1, 15, 18, 0)]})  # a Monday

    result = add_calendar_features(df)

    assert result["pickup_date"].item() == datetime(2024, 1, 15).date()
    assert result["hour_of_day"].item() == 18
    assert result["weekday"].item() == 1
    assert result["weekday_name"].item() == "Monday"


def test_add_lag_feature_shifts_backward_not_forward():
    df = pl.DataFrame(
        {
            "pickup_hour": [
                datetime(2024, 1, 1, h, 0) for h in range(4)
            ],  # 4 consecutive hours
            "pickup_location_id": [1, 1, 1, 1],
            "pickup_count": [10, 20, 30, 40],
        }
    )

    result = add_lag_feature(df, lag_hours=1)

    assert result.sort("pickup_hour")["lag_1h"].to_list() == [None, 10, 20, 30]


def test_add_lag_feature_does_not_mix_zones():
    df = pl.DataFrame(
        {
            "pickup_hour": [
                datetime(2024, 1, 1, 0, 0),
                datetime(2024, 1, 1, 0, 0),
                datetime(2024, 1, 1, 1, 0),
                datetime(2024, 1, 1, 1, 0),
            ],
            "pickup_location_id": [1, 2, 1, 2],
            "pickup_count": [100, 5, 110, 6],
        }
    )

    result = add_lag_feature(df, lag_hours=1)

    zone_1_hour_2 = result.filter(
        (pl.col("pickup_location_id") == 1)
        & (pl.col("pickup_hour") == datetime(2024, 1, 1, 1, 0))
    )
    assert (
        zone_1_hour_2["lag_1h"].item() == 100
    )  # zone 1's own prior value, not zone 2's (5)


def test_add_lag_feature_null_for_first_observation_per_zone():
    df = pl.DataFrame(
        {
            "pickup_hour": [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 0, 0)],
            "pickup_location_id": [1, 2],
            "pickup_count": [10, 20],
        }
    )

    result = add_lag_feature(df, lag_hours=1)

    assert result["lag_1h"].is_null().all()
