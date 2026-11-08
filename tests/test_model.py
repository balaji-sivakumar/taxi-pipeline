# ruff: noqa: DTZ001 -- naive datetimes here deliberately mirror quality.py's
# own naive comparisons against TLC's tz-naive timestamp columns.
import datetime

import polars as pl

from taxi_pipeline.model import (
    feature_columns,
    load_model_artifacts,
    one_hot_encode_zones,
    predict_next_day,
    predict_next_day_from_artifacts,
    prepare_model_data,
    train_and_save_model,
    train_demand_model,
    train_on_all_data,
)


def test_one_hot_encode_zones_creates_one_column_per_zone():
    df = pl.DataFrame({"pickup_location_id": [1, 2, 1]})

    result = one_hot_encode_zones(df, zones=[1, 2, 3])

    assert result["zone_1"].to_list() == [1, 0, 1]
    assert result["zone_2"].to_list() == [0, 1, 0]
    assert result["zone_3"].to_list() == [0, 0, 0]


def test_feature_columns_includes_zone_and_other_features():
    cols = feature_columns(zones=[1, 2])

    assert cols == [
        "zone_1",
        "zone_2",
        "hour_of_day",
        "weekday",
        "lag_168h",
        "historical_avg",
    ]


def _synthetic_curated(num_weeks=3):
    # dense, gapless hourly rows -- add_lag_feature's own precondition (Lesson 17) --
    # spanning multiple full weeks so historical_avg's leave-one-out (needs >=2
    # occurrences of the same (zone, hour, weekday) within training) is satisfiable.
    total_hours = num_weeks * 7 * 24
    hours = [
        datetime.datetime(2024, 1, 1, 0, 0) + datetime.timedelta(hours=h)
        for h in range(total_hours)
    ]
    rows = []
    for zone in (1, 2):
        for i, hour in enumerate(hours):
            rows.append(
                {
                    "pickup_hour": hour,
                    "pickup_location_id": zone,
                    "pickup_count": 100 + zone * 10 + i,
                }
            )
    return pl.DataFrame(rows)


def test_prepare_model_data_produces_matching_zone_columns_in_train_and_test():
    # 4 weeks: week 1 drops entirely (no prior data for lag_168h), leaving weeks
    # 2-3 in train (each (zone,hour,weekday) combo then appears twice, satisfying
    # leave-one-out's group_count > 1 requirement) and week 4 as test.
    curated = _synthetic_curated(num_weeks=4)

    train, test, _baseline_table, zones = prepare_model_data(
        curated, datetime.date(2024, 1, 22)
    )

    assert train.height > 0
    assert test.height > 0
    assert set(zones) == {1, 2}
    assert all(f"zone_{z}" in train.columns for z in zones)
    assert all(f"zone_{z}" in test.columns for z in zones)


def test_train_demand_model_produces_a_working_model():
    curated = _synthetic_curated(num_weeks=4)
    train, test, _baseline_table, zones = prepare_model_data(
        curated, datetime.date(2024, 1, 22)
    )

    model = train_demand_model(train, zones)
    X_test = test.select(feature_columns(zones)).to_numpy()
    preds = model.predict(X_test)

    assert len(preds) == test.height


def test_predict_next_day_predicts_the_day_after_the_last_real_hour():
    curated = _synthetic_curated(num_weeks=4)
    last_date = curated["pickup_hour"].max().date()

    predictions = predict_next_day(curated)

    predicted_dates = predictions["pickup_hour"].dt.date().unique().to_list()
    assert predicted_dates == [last_date + datetime.timedelta(days=1)]


def test_predict_next_day_covers_every_zone_and_hour_with_no_nulls():
    curated = _synthetic_curated(num_weeks=4)

    predictions = predict_next_day(curated)

    assert predictions.height == 24 * 2  # 24 hours x 2 zones
    assert set(predictions["pickup_location_id"].unique().to_list()) == {1, 2}
    assert predictions["predicted_pickups"].is_null().sum() == 0


def test_train_on_all_data_returns_model_zones_and_baseline_table():
    curated = _synthetic_curated(num_weeks=4)

    model, zones, baseline_table = train_on_all_data(curated)

    assert zones == [1, 2]
    assert baseline_table.height > 0
    assert hasattr(model, "predict")


def test_train_and_save_model_round_trips_through_load_model_artifacts(tmp_path):
    curated = _synthetic_curated(num_weeks=4)
    model_path = tmp_path / "demand_model.joblib"

    saved_path = train_and_save_model(curated, model_path=model_path)
    artifacts = load_model_artifacts(model_path)

    assert saved_path == model_path
    assert model_path.exists()
    assert set(artifacts.keys()) == {"model", "zones", "baseline_table", "lag_hours"}
    assert artifacts["zones"] == [1, 2]


def test_predict_next_day_from_artifacts_matches_predict_next_day(tmp_path):
    # Lesson 24: the split train/serve path must produce identical
    # predictions to the original monolithic predict_next_day() -- the
    # refactor changed *when* training happens, not what it computes.
    curated = _synthetic_curated(num_weeks=4)
    model_path = tmp_path / "demand_model.joblib"
    train_and_save_model(curated, model_path=model_path)
    artifacts = load_model_artifacts(model_path)

    from_artifacts = predict_next_day_from_artifacts(curated, artifacts).sort(
        ["pickup_hour", "pickup_location_id"]
    )
    monolithic = predict_next_day(curated).sort(["pickup_hour", "pickup_location_id"])

    assert from_artifacts.equals(monolithic)
