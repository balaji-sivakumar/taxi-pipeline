from unittest.mock import MagicMock, patch

import polars as pl

from taxi_pipeline.manifest import is_processed, record_processed
from taxi_pipeline.pipeline import process_month


def _patched_stages():
    valid_df = pl.DataFrame({"a": [1, 2]})
    rejected_df = pl.DataFrame({"a": [3]})
    cleaned = pl.DataFrame({"b": [1, 2]})
    demand = pl.DataFrame({"pickup_count": [1, 2, 3]})

    return (
        patch("taxi_pipeline.pipeline.download_month", return_value="fake_raw.parquet"),
        patch("polars.scan_parquet", return_value=MagicMock()),
        patch(
            "taxi_pipeline.pipeline.apply_quality_rules",
            return_value=(valid_df, rejected_df),
        ),
        patch("taxi_pipeline.pipeline.write_rejected"),
        patch("taxi_pipeline.pipeline.clean_trips", return_value=cleaned),
        patch("taxi_pipeline.pipeline.aggregate_hourly_demand", return_value=demand),
        patch("taxi_pipeline.pipeline.write_curated"),
    )


def test_process_month_skips_when_already_processed(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    record_processed(2024, 1, {"valid_rows": 1}, manifest_path)

    with patch("taxi_pipeline.pipeline.download_month") as mock_download:
        result = process_month(2024, 1, manifest_path=manifest_path)

    assert result == {"skipped": True}
    mock_download.assert_not_called()


def test_process_month_runs_full_pipeline_when_not_processed(tmp_path):
    manifest_path = tmp_path / "manifest.json"

    patchers = _patched_stages()
    with (
        patchers[0] as mock_download,
        patchers[1],
        patchers[2],
        patchers[3],
        patchers[4],
        patchers[5],
        patchers[6],
    ):
        result = process_month(2024, 1, manifest_path=manifest_path)

    mock_download.assert_called_once_with(2024, 1)
    assert result["valid_rows"] == 2
    assert result["rejected_rows"] == 1
    assert result["curated_rows"] == 3
    assert is_processed(2024, 1, manifest_path) is True


def test_process_month_force_reprocesses_even_if_already_processed(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    record_processed(2024, 1, {"valid_rows": 999}, manifest_path)

    patchers = _patched_stages()
    with (
        patchers[0] as mock_download,
        patchers[1],
        patchers[2],
        patchers[3],
        patchers[4],
        patchers[5],
        patchers[6],
    ):
        result = process_month(2024, 1, force=True, manifest_path=manifest_path)

    mock_download.assert_called_once()
    assert result["valid_rows"] == 2
