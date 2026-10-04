from pathlib import Path

import polars as pl

# Recorded from yellow_tripdata_2024-01.parquet (NYC TLC), inspected 2026-10-03.
# This is our explicit record of the source's data contract. If a future
# month's file doesn't match this, we want a loud failure, not silent drift.
YELLOW_TRIPDATA_SCHEMA: dict[str, pl.DataType] = {
    "VendorID": pl.Int32,
    "tpep_pickup_datetime": pl.Datetime("ns"),
    "tpep_dropoff_datetime": pl.Datetime("ns"),
    "passenger_count": pl.Int64,
    "trip_distance": pl.Float64,
    "RatecodeID": pl.Int64,
    "store_and_fwd_flag": pl.String,
    "PULocationID": pl.Int32,
    "DOLocationID": pl.Int32,
    "payment_type": pl.Int64,
    "fare_amount": pl.Float64,
    "extra": pl.Float64,
    "mta_tax": pl.Float64,
    "tip_amount": pl.Float64,
    "tolls_amount": pl.Float64,
    "improvement_surcharge": pl.Float64,
    "total_amount": pl.Float64,
    "congestion_surcharge": pl.Float64,
    "Airport_fee": pl.Float64,
}


def validate_schema(path: Path) -> list[str]:
    """Compare a Parquet file's actual schema against YELLOW_TRIPDATA_SCHEMA.

    Returns a list of human-readable differences; an empty list means the
    file matches our recorded data contract exactly.
    """
    actual = pl.scan_parquet(path).collect_schema()
    expected_cols = set(YELLOW_TRIPDATA_SCHEMA)
    actual_cols = set(actual)

    differences = [
        f"missing expected column: {name}"
        for name in sorted(expected_cols - actual_cols)
    ]
    differences += [
        f"unexpected new column: {name}" for name in sorted(actual_cols - expected_cols)
    ]

    for name in sorted(expected_cols & actual_cols):
        expected_dtype = YELLOW_TRIPDATA_SCHEMA[name]
        actual_dtype = actual[name]
        if actual_dtype != expected_dtype:
            differences.append(f"{name}: expected {expected_dtype}, got {actual_dtype}")

    return differences
