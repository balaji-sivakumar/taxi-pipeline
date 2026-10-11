import json
from pathlib import Path

MANIFEST_PATH = Path("data/manifest.json")


def _key(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def load_manifest(manifest_path: Path = MANIFEST_PATH) -> dict:
    if not manifest_path.exists():
        return {}
    return json.loads(manifest_path.read_text())


def save_manifest(manifest: dict, manifest_path: Path = MANIFEST_PATH) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def is_processed(year: int, month: int, manifest_path: Path = MANIFEST_PATH) -> bool:
    return _key(year, month) in load_manifest(manifest_path)


def record_processed(
    year: int, month: int, info: dict, manifest_path: Path = MANIFEST_PATH
) -> None:
    manifest = load_manifest(manifest_path)
    manifest[_key(year, month)] = info
    save_manifest(manifest, manifest_path)
