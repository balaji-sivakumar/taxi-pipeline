import datetime
import operator
from functools import reduce
from pathlib import Path

import polars as pl

REJECTED_DIR = Path("data/rejected")


def build_quality_rules(year: int, month: int) -> dict[str, pl.Expr]:
    """Named, documented boolean rules defining what counts as a valid trip record. See DECISIONS.md for reasoning behind each bound."""
    # Naive datetimes deliberately: tpep_pickup_datetime's own dtype is tz-naive
    # and TLC never documents a timezone for it, so asserting one here would be
    # inventing a fact, not fixing one, and would break comparison against the column.
    month_start = datetime.datetime(year, month, 1)  # noqa: DTZ001
    month_end = (
        datetime.datetime(year + 1, 1, 1)  # noqa: DTZ001
        if month == 12
        else datetime.datetime(year, month + 1, 1)  # noqa: DTZ001
    )

    return {
        "pickup_within_file_month": (pl.col("tpep_pickup_datetime") >= month_start)
        & (pl.col("tpep_pickup_datetime") < month_end),
        "dropoff_not_before_pickup": pl.col("tpep_dropoff_datetime")
        >= pl.col("tpep_pickup_datetime"),
        "trip_distance_plausible": (pl.col("trip_distance") >= 0)
        & (pl.col("trip_distance") <= 100),
        "fare_amount_non_negative": pl.col("fare_amount") >= 0,
        "total_amount_non_negative": pl.col("total_amount") >= 0,
        "passenger_count_plausible": pl.col("passenger_count").is_null()
        | ((pl.col("passenger_count") >= 1) & (pl.col("passenger_count") <= 9)),
    }


def apply_quality_rules(
    lf: pl.LazyFrame, year: int, month: int
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split records into (valid, rejected); rejected rows carry failed_rules, the list of rule names that record failed."""
    rules = build_quality_rules(year, month)
    original_cols = lf.collect_schema().names()

    flagged = lf.with_columns(
        **{f"__rule_{name}": expr for name, expr in rules.items()}
    )

    is_valid = reduce(operator.and_, (pl.col(f"__rule_{name}") for name in rules))
    failed_rules = pl.concat_list(
        [
            pl.when(~pl.col(f"__rule_{name}")).then(pl.lit(name)).otherwise(None)
            for name in rules
        ]
    ).list.drop_nulls()

    flagged = flagged.with_columns(__is_valid=is_valid, failed_rules=failed_rules)

    valid_df = flagged.filter(pl.col("__is_valid")).select(original_cols).collect()
    rejected_df = (
        flagged.filter(~pl.col("__is_valid"))
        .select([*original_cols, "failed_rules"])
        .collect()
    )
    return valid_df, rejected_df


def rejected_path_for(year: int, month: int, rejected_dir: Path = REJECTED_DIR) -> Path:
    return (
        rejected_dir
        / f"year={year:04d}"
        / f"month={month:02d}"
        / f"yellow_tripdata_{year:04d}-{month:02d}_rejected.parquet"
    )


def write_rejected(
    rejected_df: pl.DataFrame, year: int, month: int, rejected_dir: Path = REJECTED_DIR
) -> Path:
    target = rejected_path_for(year, month, rejected_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    rejected_df.write_parquet(target)
    return target
