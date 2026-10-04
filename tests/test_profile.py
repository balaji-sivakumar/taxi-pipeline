import polars as pl

from taxi_pipeline.profile import profile_parquet


def _profile_by_column(path):
    return {p["column"]: p for p in profile_parquet(path)}


def test_profile_counts_nulls_correctly(tmp_path):
    path = tmp_path / "nulls.parquet"
    pl.DataFrame({"a": [1, None, 3, None]}, schema={"a": pl.Int64}).write_parquet(path)

    profiles = _profile_by_column(path)

    assert profiles["a"]["null_count"] == 2
    assert profiles["a"]["null_fraction"] == 0.5


def test_profile_computes_min_max_for_numeric_columns(tmp_path):
    path = tmp_path / "numeric.parquet"
    pl.DataFrame({"a": [5, -3, 10, 1]}, schema={"a": pl.Int64}).write_parquet(path)

    profiles = _profile_by_column(path)

    assert profiles["a"]["min"] == -3
    assert profiles["a"]["max"] == 10


def test_profile_computes_min_max_for_datetime_columns(tmp_path):
    path = tmp_path / "datetime.parquet"
    df = pl.DataFrame(
        {"a": ["2024-01-01", "2002-12-31", "2024-01-15"]},
        schema={"a": pl.Utf8},
    ).with_columns(pl.col("a").str.to_datetime())
    df.write_parquet(path)

    profiles = _profile_by_column(path)

    assert str(profiles["a"]["min"]) == "2002-12-31 00:00:00"
    assert str(profiles["a"]["max"]) == "2024-01-15 00:00:00"


def test_profile_skips_min_max_for_string_columns(tmp_path):
    path = tmp_path / "strings.parquet"
    pl.DataFrame({"a": ["Y", "N", None]}, schema={"a": pl.Utf8}).write_parquet(path)

    profiles = _profile_by_column(path)

    assert profiles["a"]["min"] is None
    assert profiles["a"]["max"] is None
    assert profiles["a"]["null_count"] == 1
