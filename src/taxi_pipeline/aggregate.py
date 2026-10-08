import datetime

import polars as pl

from taxi_pipeline.dates import month_bounds


def aggregate_hourly_demand(
    cleaned_df: pl.DataFrame, year: int, month: int
) -> pl.DataFrame:
    """One row per (pickup_hour, pickup_location_id) for every hour in the month and every
    zone observed anywhere in cleaned_df -- pickup_count is 0, not missing, where nothing happened.

    Note: "every zone" means every zone observed in this month's data, not the full official
    NYC TLC zone list (not yet loaded) -- a zone with zero pickups for the whole month is still
    absent. That's a real, documented limitation, not an oversight.
    """
    month_start, month_end = month_bounds(year, month)
    last_hour = month_end - datetime.timedelta(hours=1)

    all_hours = pl.DataFrame(
        {
            "pickup_hour": pl.datetime_range(
                month_start, last_hour, interval="1h", eager=True
            )
        }
    )
    all_zones = cleaned_df.select("pickup_location_id").unique()
    grid = all_hours.join(all_zones, how="cross")

    counts = cleaned_df.group_by("pickup_hour", "pickup_location_id").agg(
        pl.len().alias("pickup_count")
    )

    return (
        grid.join(counts, on=["pickup_hour", "pickup_location_id"], how="left")
        .with_columns(pl.col("pickup_count").fill_null(0))
        .sort("pickup_hour", "pickup_location_id")
    )
