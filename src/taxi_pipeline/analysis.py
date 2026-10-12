from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl

from taxi_pipeline.curated import CURATED_DIR

ZONE_LOOKUP_PATH = Path("data/reference/taxi_zone_lookup.csv")


def load_curated(curated_dir: Path = CURATED_DIR) -> pl.DataFrame:
    return pl.read_parquet(f"{curated_dir}/year=*/month=*/*.parquet")


def zone_totals(
    curated_df: pl.DataFrame, lookup_path: Path = ZONE_LOOKUP_PATH
) -> pl.DataFrame:
    """Total pickups per zone over the whole dataset, with real zone names joined in.

    A single grouping step, deliberately: unlike hourly_profile/day_of_week_profile,
    this answers "what's the grand total" rather than "what's typical", so there's
    no need for the sum-then-average two-step -- one sum is the whole answer.
    """
    lookup = pl.read_csv(lookup_path)
    return (
        curated_df.group_by("pickup_location_id")
        .agg(pl.col("pickup_count").sum().alias("total_pickups"))
        .join(lookup, left_on="pickup_location_id", right_on="LocationID")
        .sort("total_pickups", descending=True)
    )


def hourly_profile(curated_df: pl.DataFrame) -> pl.DataFrame:
    """Average citywide pickups per hour-of-day (0-23), across all days and zones.

    Two-step aggregation, deliberately: first collapse zone (sum -> one
    citywide total per actual date+hour), then collapse date (mean across
    days) -- averaging, not summing, at the second step is what makes this
    comparable across hours regardless of how many days are in the dataset.
    """
    return (
        curated_df.with_columns(
            pickup_date=pl.col("pickup_hour").dt.date(),
            hour_of_day=pl.col("pickup_hour").dt.hour(),
        )
        .group_by("pickup_date", "hour_of_day")
        .agg(pl.col("pickup_count").sum().alias("citywide_pickups"))
        .group_by("hour_of_day")
        .agg(pl.col("citywide_pickups").mean().alias("avg_citywide_pickups"))
        .sort("hour_of_day")
    )


def day_of_week_profile(curated_df: pl.DataFrame) -> pl.DataFrame:
    """Average citywide pickups per day-of-week, across all weeks and zones.

    Mean (not sum) across dates is required here, not just preferred: the
    dataset's date range may not contain an equal number of each weekday,
    so summing would bias the comparison toward whichever weekday happens
    to occur more often in the data.
    """
    return (
        curated_df.with_columns(
            pickup_date=pl.col("pickup_hour").dt.date(),
            weekday=pl.col("pickup_hour").dt.weekday(),
            weekday_name=pl.col("pickup_hour").dt.strftime("%A"),
        )
        .group_by("pickup_date", "weekday", "weekday_name")
        .agg(pl.col("pickup_count").sum().alias("citywide_pickups"))
        .group_by("weekday", "weekday_name")
        .agg(pl.col("citywide_pickups").mean().alias("avg_citywide_pickups"))
        .sort("weekday")
    )


def plot_hourly_profile(profile: pl.DataFrame, save_path: Path) -> Path:
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(profile["hour_of_day"], profile["avg_citywide_pickups"], marker="o")
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Average citywide pickups")
    ax.set_title("Average hourly pickup demand (Jan-Feb 2024)")
    ax.set_xticks(range(0, 24, 2))
    fig.tight_layout()
    fig.savefig(save_path)
    plt.close(fig)
    return save_path


def plot_day_of_week_profile(profile: pl.DataFrame, save_path: Path) -> Path:
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(profile["weekday_name"], profile["avg_citywide_pickups"])
    ax.set_xlabel("Day of week")
    ax.set_ylabel("Average citywide pickups")
    ax.set_title("Average daily pickup demand by weekday (Jan-Feb 2024)")
    fig.tight_layout()
    fig.savefig(save_path)
    plt.close(fig)
    return save_path


def plot_zone_totals(totals: pl.DataFrame, save_path: Path, top_n: int = 15) -> Path:
    top = totals.head(top_n)
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(top["Zone"], top["total_pickups"])
    ax.invert_yaxis()
    ax.set_xlabel("Total pickups (Jan-Feb 2024)")
    ax.set_ylabel("Zone")
    ax.set_title(f"Top {top_n} zones by total pickups")
    fig.tight_layout()
    fig.savefig(save_path)
    plt.close(fig)
    return save_path
