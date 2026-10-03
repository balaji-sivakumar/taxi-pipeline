from taxi_pipeline.schema import YELLOW_TRIPDATA_SCHEMA


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
