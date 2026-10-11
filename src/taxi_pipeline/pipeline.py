import datetime
import logging
from pathlib import Path

import polars as pl

from taxi_pipeline.aggregate import aggregate_hourly_demand
from taxi_pipeline.curated import write_curated
from taxi_pipeline.ingest import download_month
from taxi_pipeline.manifest import MANIFEST_PATH, load_manifest, record_processed
from taxi_pipeline.quality import apply_quality_rules, write_rejected
from taxi_pipeline.transform import clean_trips

logger = logging.getLogger(__name__)


def process_month(
    year: int, month: int, force: bool = False, manifest_path: Path = MANIFEST_PATH
) -> dict:
    """Run download -> validate -> clean -> aggregate -> curate for one month.

    download_month() always runs first (a cheap HEAD check even when it ends up
    skipping the actual download), so we can detect a source file that changed
    since we last processed it -- not just whether this month is in the manifest.
    Skips the compute stages only if the manifest already records this exact
    raw_file_size as processed, unless force=True.
    """
    raw_path = download_month(year, month)
    raw_file_size = raw_path.stat().st_size

    key = f"{year:04d}-{month:02d}"
    previous = load_manifest(manifest_path).get(key)

    if not force and previous is not None:
        if previous.get("raw_file_size") == raw_file_size:
            logger.info("%s already processed, skipping", key)
            return {"skipped": True}
        logger.warning(
            "%s source file changed since last processing (size %s -> %s), reprocessing",
            key,
            previous.get("raw_file_size"),
            raw_file_size,
        )

    lf = pl.scan_parquet(raw_path)
    valid_df, rejected_df = apply_quality_rules(lf, year, month)
    write_rejected(rejected_df, year, month)
    cleaned = clean_trips(valid_df)
    demand = aggregate_hourly_demand(cleaned, year, month)
    write_curated(demand, year, month)

    info = {
        "processed_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "raw_file_size": raw_file_size,
        "valid_rows": valid_df.height,
        "rejected_rows": rejected_df.height,
        "curated_rows": demand.height,
    }
    record_processed(year, month, info, manifest_path)
    return info


def process_months(
    months: list[tuple[int, int]],
    force: bool = False,
    manifest_path: Path = MANIFEST_PATH,
) -> list[dict]:
    """Process each month independently -- one month failing is recorded and
    logged, not swallowed, but does not abort the rest of the backfill."""
    results = []
    for year, month in months:
        try:
            result = process_month(
                year, month, force=force, manifest_path=manifest_path
            )
        except Exception as exc:  # noqa: BLE001 -- isolating one month's failure from the rest of the backfill is the point; a narrower type would defeat it
            logger.error("failed to process %04d-%02d: %s", year, month, exc)
            result = {"error": str(exc)}
        results.append(result)
    return results
