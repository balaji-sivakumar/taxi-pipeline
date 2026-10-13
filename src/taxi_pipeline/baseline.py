import datetime

import polars as pl

from taxi_pipeline.features import add_calendar_features


def split_train_test(
    curated_df: pl.DataFrame, cutoff_date: datetime.date
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Train = everything before cutoff_date; test = everything from cutoff_date onward."""
    df = add_calendar_features(curated_df)
    train = df.filter(pl.col("pickup_date") < cutoff_date)
    test = df.filter(pl.col("pickup_date") >= cutoff_date)
    return train, test


def build_baseline(train_df: pl.DataFrame) -> pl.DataFrame:
    """The baseline 'model': one average pickup_count per (zone, hour, weekday) group."""
    return train_df.group_by("pickup_location_id", "hour_of_day", "weekday").agg(
        pl.col("pickup_count").mean().alias("predicted_pickups")
    )


def predict_baseline(test_df: pl.DataFrame, baseline: pl.DataFrame) -> pl.DataFrame:
    """Look up each test row's prediction from the baseline table -- no recomputation."""
    return test_df.join(
        baseline, on=["pickup_location_id", "hour_of_day", "weekday"], how="left"
    )


def mean_absolute_error(predictions_df: pl.DataFrame) -> dict:
    """MAE over rows with a prediction, plus explicit counts of what was excluded.

    Filters nulls out explicitly before averaging -- not relying on Polars'
    default null-skipping behavior in .mean(), so the exclusion is visible in
    the code and reported in the result, not a silent side effect.
    """
    total_rows = predictions_df.height
    valid = predictions_df.filter(pl.col("predicted_pickups").is_not_null())

    mae = valid.select(
        (pl.col("pickup_count") - pl.col("predicted_pickups")).abs().mean()
    ).item()

    return {
        "mae": mae,
        "rows_used": valid.height,
        "rows_excluded": total_rows - valid.height,
        "total_rows": total_rows,
    }
