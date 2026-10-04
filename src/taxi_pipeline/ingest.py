import logging
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")
SOURCE_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"


def raw_path_for(year: int, month: int, raw_dir: Path = RAW_DIR) -> Path:
    return (
        raw_dir
        / f"year={year:04d}"
        / f"month={month:02d}"
        / f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    )


def download_month(year: int, month: int, raw_dir: Path = RAW_DIR) -> Path:
    url = SOURCE_URL_TEMPLATE.format(year=year, month=month)
    target = raw_path_for(year, month, raw_dir)

    head = requests.head(url, timeout=30)
    head.raise_for_status()
    expected_size = int(head.headers["content-length"])

    if target.exists() and target.stat().st_size == expected_size:
        logger.info(
            "already have %s (%d bytes) - skipping download", target, expected_size
        )
        return target

    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_suffix(target.suffix + ".tmp")

    with requests.get(url, stream=True, timeout=30) as response:
        response.raise_for_status()
        with open(tmp_path, "wb") as f:
            f.writelines(response.iter_content(chunk_size=1024 * 1024))

    actual_size = tmp_path.stat().st_size
    if actual_size != expected_size:
        tmp_path.unlink()
        raise RuntimeError(
            f"download size mismatch for {url}: expected {expected_size}, got {actual_size}"
        )

    tmp_path.rename(target)
    logger.info("downloaded %s (%d bytes)", target, actual_size)
    return target
