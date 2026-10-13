import polars as pl


def add_calendar_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pickup_date=pl.col("pickup_hour").dt.date(),
        hour_of_day=pl.col("pickup_hour").dt.hour(),
        weekday=pl.col("pickup_hour").dt.weekday(),
        weekday_name=pl.col("pickup_hour").dt.strftime("%A"),
    )
