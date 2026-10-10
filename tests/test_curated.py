from datetime import datetime

import polars as pl

from taxi_pipeline.curated import curated_path_for, write_curated


def _demand_df(count: int = 5):
    return pl.DataFrame(
        {
            "pickup_hour": [datetime(2024, 1, 1, 0, 0, 0)] * count,  # noqa: DTZ001
            "pickup_location_id": list(range(count)),
            "pickup_count": [10] * count,
        }
    )


def test_write_curated_persists_to_expected_partitioned_path(tmp_path):
    path = write_curated(_demand_df(), 2024, 1, curated_dir=tmp_path)

    assert path == curated_path_for(2024, 1, tmp_path)
    assert path.exists()
    assert pl.read_parquet(path).height == 5


def test_write_curated_replaces_rather_than_merges(tmp_path):
    write_curated(_demand_df(count=5), 2024, 1, curated_dir=tmp_path)
    path = write_curated(_demand_df(count=2), 2024, 1, curated_dir=tmp_path)

    assert pl.read_parquet(path).height == 2


def test_write_curated_leaves_no_leftover_tmp_file(tmp_path):
    path = write_curated(_demand_df(), 2024, 1, curated_dir=tmp_path)

    assert not path.with_suffix(path.suffix + ".tmp").exists()
