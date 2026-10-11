from unittest.mock import MagicMock, patch

import polars as pl

from taxi_pipeline.manifest import load_manifest, record_processed
from taxi_pipeline.pipeline import process_month, process_months


def _patched_stages(raw_path):
    valid_df = pl.DataFrame({"a": [1, 2]})
    rejected_df = pl.DataFrame({"a": [3]})
    cleaned = pl.DataFrame({"b": [1, 2]})
    demand = pl.DataFrame({"pickup_count": [1, 2, 3]})

    return (
        patch("taxi_pipeline.pipeline.download_month", return_value=raw_path),
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


def _run_with_patches(raw_path, *args, **kwargs):
    patchers = _patched_stages(raw_path)
    with (
        patchers[0] as mock_download,
        patchers[1],
        patchers[2],
        patchers[3],
        patchers[4],
        patchers[5],
        patchers[6],
    ):
        result = process_month(*args, **kwargs)
    return result, mock_download


def test_process_month_skips_when_raw_file_size_matches(tmp_path):
    raw_path = tmp_path / "raw.parquet"
    raw_path.write_bytes(b"x" * 100)
    manifest_path = tmp_path / "manifest.json"
    record_processed(2024, 1, {"raw_file_size": 100}, manifest_path)

    result, mock_download = _run_with_patches(
        raw_path, 2024, 1, manifest_path=manifest_path
    )

    assert result == {"skipped": True}
    mock_download.assert_called_once_with(2024, 1)


def test_process_month_detects_changed_source_and_reprocesses(tmp_path):
    raw_path = tmp_path / "raw.parquet"
    raw_path.write_bytes(b"x" * 999)
    manifest_path = tmp_path / "manifest.json"
    record_processed(2024, 1, {"raw_file_size": 100}, manifest_path)

    result, _ = _run_with_patches(raw_path, 2024, 1, manifest_path=manifest_path)

    assert result["raw_file_size"] == 999
    assert result["valid_rows"] == 2


def test_process_month_runs_full_pipeline_when_not_processed(tmp_path):
    raw_path = tmp_path / "raw.parquet"
    raw_path.write_bytes(b"x" * 50)
    manifest_path = tmp_path / "manifest.json"

    result, mock_download = _run_with_patches(
        raw_path, 2024, 1, manifest_path=manifest_path
    )

    mock_download.assert_called_once_with(2024, 1)
    assert result["valid_rows"] == 2
    assert result["rejected_rows"] == 1
    assert result["curated_rows"] == 3
    assert load_manifest(manifest_path)["2024-01"]["raw_file_size"] == 50


def test_process_month_force_reprocesses_even_if_already_processed(tmp_path):
    raw_path = tmp_path / "raw.parquet"
    raw_path.write_bytes(b"x" * 50)
    manifest_path = tmp_path / "manifest.json"
    record_processed(2024, 1, {"raw_file_size": 50}, manifest_path)

    result, mock_download = _run_with_patches(
        raw_path, 2024, 1, force=True, manifest_path=manifest_path
    )

    mock_download.assert_called_once()
    assert result["valid_rows"] == 2


def test_process_months_continues_past_one_failure(tmp_path):
    manifest_path = tmp_path / "manifest.json"

    def fake_download(year, month):
        if month == 6:
            raise RuntimeError("source not found")
        path = tmp_path / f"{year}-{month}.parquet"
        path.write_bytes(b"x" * 10)
        return path

    patchers = _patched_stages(None)
    with (
        patch("taxi_pipeline.pipeline.download_month", side_effect=fake_download),
        patchers[1],
        patchers[2],
        patchers[3],
        patchers[4],
        patchers[5],
        patchers[6],
    ):
        results = process_months(
            [(2024, 1), (2024, 6), (2024, 2)], manifest_path=manifest_path
        )

    assert "error" in results[1]
    assert results[0]["valid_rows"] == 2
    assert results[2]["valid_rows"] == 2
