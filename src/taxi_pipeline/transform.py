import duckdb
import polars as pl


def clean_trips(valid_df: pl.DataFrame) -> pl.DataFrame:
    """Rename source columns to curated-layer names and derive pickup_hour for aggregation."""
    return duckdb.sql(
        """
        SELECT
            tpep_pickup_datetime AS pickup_datetime,
            date_trunc('hour', tpep_pickup_datetime) AS pickup_hour,
            PULocationID AS pickup_location_id,
            DOLocationID AS dropoff_location_id,
            trip_distance,
            passenger_count,
            fare_amount,
            total_amount
        FROM valid_df
        """
    ).pl()
