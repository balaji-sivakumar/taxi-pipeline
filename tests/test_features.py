# ruff: noqa: DTZ001 -- naive datetimes here deliberately mirror quality.py's
# own naive comparisons against TLC's tz-naive timestamp columns.
from datetime import datetime

import polars as pl

from taxi_pipeline.features import (
    add_calendar_features,
    add_historical_avg_feature,
    add_lag_feature,
)


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


def test_add_historical_avg_feature_with_baseline_table_is_a_lookup():
    baseline_table = pl.DataFrame(
        {
            "pickup_location_id": [132],
            "hour_of_day": [18],
            "weekday": [2],
            "predicted_pickups": [227.25],
        }
    )
    test_df = pl.DataFrame(
        {
            "pickup_location_id": [132],
            "hour_of_day": [18],
            "weekday": [2],
            "pickup_count": [205],
        }
    )

    result = add_historical_avg_feature(test_df, baseline_table)

    assert result["historical_avg"].item() == 227.25


def test_add_historical_avg_feature_leave_one_out_excludes_own_value():
    # Matches the hand-verified JFK Tuesday 6pm example: 223, 273, 220, 193
    df = pl.DataFrame(
        {
            "pickup_location_id": [132, 132, 132, 132],
            "hour_of_day": [18, 18, 18, 18],
            "weekday": [2, 2, 2, 2],
            "pickup_count": [223, 273, 220, 193],
        }
    )

    result = add_historical_avg_feature(df)

    expected = [
        (273 + 220 + 193) / 3,
        (223 + 220 + 193) / 3,
        (223 + 273 + 193) / 3,
        (223 + 273 + 220) / 3,
    ]
    actual = [
        result.filter(pl.col("pickup_count") == v)["historical_avg"].item()
        for v in [223, 273, 220, 193]
    ]
    assert actual == expected


def test_add_historical_avg_feature_null_for_single_sample_group():
    df = pl.DataFrame(
        {
            "pickup_location_id": [1],
            "hour_of_day": [0],
            "weekday": [1],
            "pickup_count": [50],
        }
    )

    result = add_historical_avg_feature(df)

    assert result["historical_avg"].item() is None
