import polars as pl

from taxi_pipeline.schema import YELLOW_TRIPDATA_SCHEMA, validate_schema


def test_schema_has_no_duplicate_columns():
    assert len(YELLOW_TRIPDATA_SCHEMA) == len(set(YELLOW_TRIPDATA_SCHEMA))


def test_schema_contains_core_columns():
    required = {
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "fare_amount",
    }
    assert required.issubset(YELLOW_TRIPDATA_SCHEMA.keys())


def _write_matching_file(path):
    df = pl.DataFrame(
        {
            name: pl.Series([], dtype=dtype)
            for name, dtype in YELLOW_TRIPDATA_SCHEMA.items()
        }
    )
    df.write_parquet(path)


def test_validate_schema_returns_no_differences_for_matching_file(tmp_path):
    path = tmp_path / "matching.parquet"
    _write_matching_file(path)

    assert validate_schema(path) == []


def test_validate_schema_detects_missing_column(tmp_path):
    schema = dict(YELLOW_TRIPDATA_SCHEMA)
    del schema["trip_distance"]
    path = tmp_path / "missing_column.parquet"
    pl.DataFrame(
        {name: pl.Series([], dtype=dtype) for name, dtype in schema.items()}
    ).write_parquet(path)

    assert validate_schema(path) == ["missing expected column: trip_distance"]


def test_validate_schema_detects_unexpected_new_column(tmp_path):
    schema = dict(YELLOW_TRIPDATA_SCHEMA)
    schema["cbd_congestion_fee"] = pl.Float64
    path = tmp_path / "extra_column.parquet"
    pl.DataFrame(
        {name: pl.Series([], dtype=dtype) for name, dtype in schema.items()}
    ).write_parquet(path)

    assert validate_schema(path) == ["unexpected new column: cbd_congestion_fee"]


def test_validate_schema_detects_type_mismatch(tmp_path):
    schema = dict(YELLOW_TRIPDATA_SCHEMA)
    schema["passenger_count"] = pl.Float64  # drifted from Int64
    path = tmp_path / "type_mismatch.parquet"
    pl.DataFrame(
        {name: pl.Series([], dtype=dtype) for name, dtype in schema.items()}
    ).write_parquet(path)

    assert validate_schema(path) == ["passenger_count: expected Int64, got Float64"]
