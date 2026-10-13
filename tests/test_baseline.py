# ruff: noqa: DTZ001 -- naive datetimes here deliberately mirror quality.py's
# own naive comparisons against TLC's tz-naive timestamp columns.
import datetime

import polars as pl

from taxi_pipeline.baseline import (
    build_baseline,
    mean_absolute_error,
    predict_baseline,
    split_train_test,
)


def test_split_train_test_separates_by_cutoff_date():
    curated = pl.DataFrame(
        {
            "pickup_hour": [
                datetime.datetime(2024, 1, 31, 10, 0),
                datetime.datetime(2024, 2, 1, 0, 0),
                datetime.datetime(2024, 2, 5, 10, 0),
            ],
            "pickup_location_id": [1, 1, 1],
            "pickup_count": [10, 20, 30],
        }
    )

    train, test = split_train_test(curated, datetime.date(2024, 2, 1))

    assert train.height == 1
    assert test.height == 2


def test_build_baseline_averages_within_each_group():
    train = pl.DataFrame(
        {
            "pickup_hour": [
                datetime.datetime(2024, 1, 2, 18, 0),  # Tuesday
                datetime.datetime(2024, 1, 9, 18, 0),  # Tuesday
            ],
            "pickup_location_id": [161, 161],
            "pickup_count": [48, 52],
            "pickup_date": [datetime.date(2024, 1, 2), datetime.date(2024, 1, 9)],
            "hour_of_day": [18, 18],
            "weekday": [2, 2],
            "weekday_name": ["Tuesday", "Tuesday"],
        }
    )

    baseline = build_baseline(train)

    assert baseline.height == 1
    assert baseline["predicted_pickups"].item() == 50.0


def test_predict_baseline_looks_up_matching_group():
    baseline = pl.DataFrame(
        {
            "pickup_location_id": [161],
            "hour_of_day": [18],
            "weekday": [2],
            "predicted_pickups": [50.0],
        }
    )
    test = pl.DataFrame(
        {
            "pickup_location_id": [161],
            "hour_of_day": [18],
            "weekday": [2],
            "pickup_count": [55],
        }
    )

    predictions = predict_baseline(test, baseline)

    assert predictions["predicted_pickups"].item() == 50.0


def test_predict_baseline_leaves_null_for_unseen_group():
    baseline = pl.DataFrame(
        {
            "pickup_location_id": [161],
            "hour_of_day": [18],
            "weekday": [2],
            "predicted_pickups": [50.0],
        }
    )
    test = pl.DataFrame(
        {
            "pickup_location_id": [99],  # zone never seen in training
            "hour_of_day": [18],
            "weekday": [2],
            "pickup_count": [5],
        }
    )

    predictions = predict_baseline(test, baseline)

    assert predictions.height == 1  # row is kept, not dropped
    assert predictions["predicted_pickups"].item() is None


def test_mean_absolute_error_computes_and_reports_exclusions():
    predictions = pl.DataFrame(
        {
            "pickup_count": [55, 46, 5],
            "predicted_pickups": [50.0, 50.0, None],  # last row unpredictable
        }
    )

    result = mean_absolute_error(predictions)

    assert result["total_rows"] == 3
    assert result["rows_used"] == 2
    assert result["rows_excluded"] == 1
    assert result["mae"] == (5.0 + 4.0) / 2  # only the two non-null rows
