import datetime
from pathlib import Path

import joblib
import polars as pl
from sklearn.ensemble import RandomForestRegressor

from taxi_pipeline.baseline import build_baseline, split_train_test
from taxi_pipeline.features import (
    add_calendar_features,
    add_historical_avg_feature,
    add_lag_feature,
)

DEFAULT_LAG_HOURS = 168
MODEL_PATH = Path("data/models/demand_model.joblib")


def one_hot_encode_zones(df: pl.DataFrame, zones: list[int]) -> pl.DataFrame:
    return df.with_columns(
        [
            (pl.col("pickup_location_id") == z).cast(pl.Int8).alias(f"zone_{z}")
            for z in zones
        ]
    )


def feature_columns(zones: list[int], lag_hours: int = DEFAULT_LAG_HOURS) -> list[str]:
    return [f"zone_{z}" for z in zones] + [
        "hour_of_day",
        "weekday",
        f"lag_{lag_hours}h",
        "historical_avg",
    ]


def prepare_model_data(
    curated_df: pl.DataFrame,
    cutoff_date: datetime.date,
    lag_hours: int = DEFAULT_LAG_HOURS,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, list[int]]:
    """Lesson 18's full pipeline: calendar+lag features, train/test split, baseline,
    historical_avg (leave-one-out for train, lookup for test), null-filtering, one-hot
    zones. Returns (train, test, baseline_table, zones) ready to train/evaluate on.

    lag_hours defaults to one real week (168) but is exposed explicitly so tests can
    use a small value with correspondingly small, dense synthetic fixtures -- the
    same reason add_lag_feature() itself exposes lag_hours (Lesson 17).
    """
    lag_col = f"lag_{lag_hours}h"
    featured = add_lag_feature(add_calendar_features(curated_df), lag_hours=lag_hours)
    train, test = split_train_test(featured, cutoff_date)

    train = train.filter(pl.col(lag_col).is_not_null())
    test = test.filter(pl.col(lag_col).is_not_null())

    baseline_table = build_baseline(train)

    train = add_historical_avg_feature(train).filter(
        pl.col("historical_avg").is_not_null()
    )
    test = add_historical_avg_feature(test, baseline_table).filter(
        pl.col("historical_avg").is_not_null()
    )

    zones = sorted(train["pickup_location_id"].unique().to_list())
    train = one_hot_encode_zones(train, zones)
    test = one_hot_encode_zones(test, zones)

    return train, test, baseline_table, zones


def train_demand_model(
    train_df: pl.DataFrame, zones: list[int], lag_hours: int = DEFAULT_LAG_HOURS
) -> RandomForestRegressor:
    """Lesson 18's validated configuration: RandomForest, one-hot zones, max_features=1 --
    the setting confirmed (via feature_importances_) to genuinely use lag_168h instead of
    just echoing historical_avg, and to beat the baseline both overall and at JFK.
    """
    X = train_df.select(feature_columns(zones, lag_hours)).to_numpy()
    y = train_df["pickup_count"].to_numpy()
    model = RandomForestRegressor(random_state=42, n_jobs=-1, max_features=1)
    model.fit(X, y)
    return model


def train_on_all_data(
    curated_df: pl.DataFrame, lag_hours: int = DEFAULT_LAG_HOURS
) -> tuple[RandomForestRegressor, list[int], pl.DataFrame]:
    """The training half of what predict_next_day() used to do in one piece
    (Lesson 19) -- split out in Lesson 24 so it can be run and persisted
    separately from the (cheap) act of serving a prediction from it.

    Trains on ALL available data, not a held-out split -- the train/test split
    exists to measure how good the model is (Lesson 18); a real deployed forecast
    uses every real observation available, since there's no more evaluation
    happening at this point, only production use.

    Returns (model, zones, baseline_table) -- everything predict_from_model()
    needs to reproduce an identical feature matrix at serving time.
    """
    lag_col = f"lag_{lag_hours}h"
    featured = add_lag_feature(add_calendar_features(curated_df), lag_hours=lag_hours)
    complete = featured.filter(pl.col(lag_col).is_not_null())

    baseline_table = build_baseline(complete)
    complete = add_historical_avg_feature(complete).filter(
        pl.col("historical_avg").is_not_null()
    )

    zones = sorted(complete["pickup_location_id"].unique().to_list())
    model = train_demand_model(one_hot_encode_zones(complete, zones), zones, lag_hours)

    return model, zones, baseline_table


def predict_from_model(
    curated_df: pl.DataFrame,
    model: RandomForestRegressor,
    zones: list[int],
    baseline_table: pl.DataFrame,
    lag_hours: int = DEFAULT_LAG_HOURS,
) -> pl.DataFrame:
    """The serving half of what predict_next_day() used to do in one piece
    (Lesson 19) -- given an ALREADY-TRAINED model (and the zones/baseline_table
    it was trained with), builds tomorrow's feature rows and predicts. No
    fitting happens here -- this is the cheap path meant to run per request.
    """
    lag_col = f"lag_{lag_hours}h"
    last_hour: datetime.datetime = curated_df["pickup_hour"].max()
    next_day = last_hour.date() + datetime.timedelta(days=1)

    future_hours = pl.DataFrame(
        {
            "pickup_hour": [
                datetime.datetime(next_day.year, next_day.month, next_day.day, h)  # noqa: DTZ001
                for h in range(24)
            ]
        }
    )
    future = add_calendar_features(
        future_hours.join(pl.DataFrame({"pickup_location_id": zones}), how="cross")
    )

    lag_source = curated_df.select(
        "pickup_location_id", "pickup_hour", "pickup_count"
    ).rename({"pickup_hour": "_lag_lookup_hour", "pickup_count": lag_col})
    future = (
        future.with_columns(
            (pl.col("pickup_hour") - pl.duration(hours=lag_hours)).alias(
                "_lag_lookup_hour"
            )
        )
        .join(lag_source, on=["pickup_location_id", "_lag_lookup_hour"], how="left")
        .drop("_lag_lookup_hour")
    )

    future = add_historical_avg_feature(future, baseline_table)
    future = one_hot_encode_zones(future, zones)

    X_future = future.select(feature_columns(zones, lag_hours)).to_numpy()
    return future.select("pickup_hour", "pickup_location_id").with_columns(
        predicted_pickups=pl.Series(model.predict(X_future))
    )


def predict_next_day(
    curated_df: pl.DataFrame, lag_hours: int = DEFAULT_LAG_HOURS
) -> pl.DataFrame:
    """Generate hourly demand predictions for the day after curated_df's last date.

    Unchanged behavior from Lesson 19 -- now composed from train_on_all_data()
    + predict_from_model() rather than one monolithic body. Still trains a
    fresh model every call; kept for exploratory/test use where that cost is
    fine. The API (Lesson 24) uses the split functions instead, so it never
    pays this cost per request.
    """
    model, zones, baseline_table = train_on_all_data(curated_df, lag_hours)
    return predict_from_model(curated_df, model, zones, baseline_table, lag_hours)


def train_and_save_model(
    curated_df: pl.DataFrame,
    lag_hours: int = DEFAULT_LAG_HOURS,
    model_path: Path = MODEL_PATH,
) -> Path:
    """Run training once and persist everything serving needs: the fitted
    model, the zones list (one-hot columns must match training exactly), and
    baseline_table (historical_avg must be looked up against the same
    averages the model learned from). Meant to run on a schedule or by hand --
    never per-request.
    """
    model, zones, baseline_table = train_on_all_data(curated_df, lag_hours)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {
            "model": model,
            "zones": zones,
            "baseline_table": baseline_table,
            "lag_hours": lag_hours,
        },
        model_path,
    )
    return model_path


def load_model_artifacts(model_path: Path = MODEL_PATH) -> dict:
    return joblib.load(model_path)


def predict_next_day_from_artifacts(
    curated_df: pl.DataFrame, artifacts: dict
) -> pl.DataFrame:
    """The actual serving path: load_model_artifacts() once, then call this
    per request -- no training happens here, only the cheap feature-building
    + predict steps.
    """
    return predict_from_model(
        curated_df,
        artifacts["model"],
        artifacts["zones"],
        artifacts["baseline_table"],
        artifacts["lag_hours"],
    )
