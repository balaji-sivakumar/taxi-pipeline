from taxi_pipeline.manifest import is_processed, load_manifest, record_processed


def test_is_processed_false_when_no_manifest_file(tmp_path):
    manifest_path = tmp_path / "manifest.json"

    assert is_processed(2024, 1, manifest_path) is False


def test_record_processed_then_is_processed_true(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    record_processed(2024, 1, {"valid_rows": 100}, manifest_path)

    assert is_processed(2024, 1, manifest_path) is True
    assert is_processed(2024, 2, manifest_path) is False


def test_record_processed_overwrites_existing_entry_for_same_month(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    record_processed(2024, 1, {"valid_rows": 100}, manifest_path)
    record_processed(2024, 1, {"valid_rows": 200}, manifest_path)

    manifest = load_manifest(manifest_path)
    assert manifest["2024-01"]["valid_rows"] == 200


def test_record_processed_preserves_other_months(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    record_processed(2024, 1, {"valid_rows": 100}, manifest_path)
    record_processed(2024, 2, {"valid_rows": 200}, manifest_path)

    manifest = load_manifest(manifest_path)
    assert set(manifest.keys()) == {"2024-01", "2024-02"}
