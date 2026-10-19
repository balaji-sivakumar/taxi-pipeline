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
