import polars as pl


def add_calendar_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pickup_date=pl.col("pickup_hour").dt.date(),
        hour_of_day=pl.col("pickup_hour").dt.hour(),
        weekday=pl.col("pickup_hour").dt.weekday(),
        weekday_name=pl.col("pickup_hour").dt.strftime("%A"),
    )


def add_lag_feature(df: pl.DataFrame, lag_hours: int = 168) -> pl.DataFrame:
    """Add pickup_count from lag_hours earlier, for the same zone.

    Shifted strictly backward (never forward -- that would leak future
    values into a feature) and computed .over("pickup_location_id") so
    one zone's history never bleeds into another's. Correct only because
    the dense grid from aggregate_hourly_demand() (Lesson 9) guarantees
    no missing hours -- a row-count shift of 168 is only truly "168 hours
    ago" if the sequence has no gaps.
    """
    return df.sort("pickup_location_id", "pickup_hour").with_columns(
        pl.col("pickup_count")
        .shift(lag_hours)
        .over("pickup_location_id")
        .alias(f"lag_{lag_hours}h")
    )


def add_historical_avg_feature(
    df: pl.DataFrame, baseline_table: pl.DataFrame | None = None
) -> pl.DataFrame:
    """Add historical_avg: the group's mean pickup_count for (zone, hour, weekday).

    If baseline_table is given, looks it up by join -- safe for test/held-out
    data, which never contributed to that average. If not given, computes a
    leave-one-out average directly from df's own rows -- required when adding
    this feature to the SAME data the average is built from, so a row's own
    value never leaks into its own feature (same leakage shape as Lesson 16's
    averaging trap, reappearing inside a feature instead of an evaluation metric).
    """
    if baseline_table is not None:
        return df.join(
            baseline_table.rename({"predicted_pickups": "historical_avg"}),
            on=["pickup_location_id", "hour_of_day", "weekday"],
            how="left",
        )

    keys = ["pickup_location_id", "hour_of_day", "weekday"]
    group_sum = pl.col("pickup_count").sum().over(keys)
    group_count = pl.col("pickup_count").count().over(keys)
    return df.with_columns(
        historical_avg=pl.when(group_count > 1)
        .then((group_sum - pl.col("pickup_count")) / (group_count - 1))
        .otherwise(None)
    )
