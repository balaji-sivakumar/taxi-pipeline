from unittest.mock import patch

import pytest
import requests

from taxi_pipeline.ingest import download_month, raw_path_for

FAKE_CONTENT = b"x" * 1000


def _mock_head(content_length: int):
    response = type("Resp", (), {})()
    response.headers = {"content-length": str(content_length)}
    response.raise_for_status = lambda: None
    return response


class _MockGetResponse:
    def __init__(self, content: bytes):
        self._content = content

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        yield self._content


def test_download_month_writes_file_when_missing(tmp_path):
    with (
        patch(
            "taxi_pipeline.ingest.requests.head",
            return_value=_mock_head(len(FAKE_CONTENT)),
        ),
        patch(
            "taxi_pipeline.ingest.requests.get",
            return_value=_MockGetResponse(FAKE_CONTENT),
        ) as mock_get,
    ):
        path = download_month(2024, 1, raw_dir=tmp_path)

    assert path == raw_path_for(2024, 1, tmp_path)
    assert path.read_bytes() == FAKE_CONTENT
    mock_get.assert_called_once()


def test_download_month_skips_when_existing_file_matches_expected_size(tmp_path):
    target = raw_path_for(2024, 1, tmp_path)
    target.parent.mkdir(parents=True)
    target.write_bytes(FAKE_CONTENT)

    with (
        patch(
            "taxi_pipeline.ingest.requests.head",
            return_value=_mock_head(len(FAKE_CONTENT)),
        ),
        patch("taxi_pipeline.ingest.requests.get") as mock_get,
    ):
        path = download_month(2024, 1, raw_dir=tmp_path)

    assert path.read_bytes() == FAKE_CONTENT
    mock_get.assert_not_called()


def test_download_month_redownloads_when_existing_file_is_corrupted(tmp_path):
    target = raw_path_for(2024, 1, tmp_path)
    target.parent.mkdir(parents=True)
    target.write_bytes(b"corrupted partial content")

    with (
        patch(
            "taxi_pipeline.ingest.requests.head",
            return_value=_mock_head(len(FAKE_CONTENT)),
        ),
        patch(
            "taxi_pipeline.ingest.requests.get",
            return_value=_MockGetResponse(FAKE_CONTENT),
        ) as mock_get,
    ):
        path = download_month(2024, 1, raw_dir=tmp_path)

    assert path.read_bytes() == FAKE_CONTENT
    mock_get.assert_called_once()


def test_download_month_recovers_from_a_transient_head_failure_via_retry(tmp_path):
    # Simulates a one-off network blip: the first HEAD call raises, the second
    # (the @task retry, Lesson 21) succeeds -- unlike the permanent-failure
    # case, this is exactly the scenario retries exist to fix.
    with (
        patch(
            "taxi_pipeline.ingest.requests.head",
            side_effect=[
                requests.exceptions.ConnectionError("simulated network blip"),
                _mock_head(len(FAKE_CONTENT)),
            ],
        ) as mock_head,
        patch(
            "taxi_pipeline.ingest.requests.get",
            return_value=_MockGetResponse(FAKE_CONTENT),
        ) as mock_get,
    ):
        path = download_month(2024, 1, raw_dir=tmp_path)

    assert path.read_bytes() == FAKE_CONTENT
    assert mock_head.call_count == 2
    mock_get.assert_called_once()


def test_download_month_raises_and_cleans_up_on_size_mismatch(tmp_path):
    with (
        patch(
            "taxi_pipeline.ingest.requests.head",
            return_value=_mock_head(len(FAKE_CONTENT) + 1),
        ),
        patch(
            "taxi_pipeline.ingest.requests.get",
            return_value=_MockGetResponse(FAKE_CONTENT),
        ),
        pytest.raises(RuntimeError, match="size mismatch"),
    ):
        download_month(2024, 1, raw_dir=tmp_path)

    target = raw_path_for(2024, 1, tmp_path)
    assert not target.exists()
    assert not target.with_suffix(target.suffix + ".tmp").exists()
