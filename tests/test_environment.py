import sys

import duckdb
import polars
import requests


def test_python_version_is_312_or_newer():
    assert sys.version_info >= (3, 12)


def test_core_dependencies_import():
    assert polars.__version__
    assert requests.__version__


def test_duckdb_can_query_a_parquet_file_directly(tmp_path):
    path = tmp_path / "sample.parquet"
    polars.DataFrame({"a": [1, 2, 3]}).write_parquet(path)

    result = duckdb.sql(f"SELECT sum(a) AS total FROM '{path}'").fetchone()

    assert result[0] == 6
