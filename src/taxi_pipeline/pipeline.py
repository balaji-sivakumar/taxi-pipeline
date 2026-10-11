import datetime
import logging
from pathlib import Path

import polars as pl

from taxi_pipeline.aggregate import aggregate_hourly_demand
from taxi_pipeline.curated import write_curated
from taxi_pipeline.ingest import download_month
from taxi_pipeline.manifest import MANIFEST_PATH, is_processed, record_processed
from taxi_pipeline.quality import apply_quality_rules, write_rejected
from taxi_pipeline.transform import clean_trips

logger = logging.getLogger(__name__)


def process_month(
    year: int, month: int, force: bool = False, manifest_path: Path = MANIFEST_PATH
) -> dict:
    """Run download -> validate -> clean -> aggregate -> curate for one month.

    Skips entirely if the manifest already records this month as processed,
    unless force=True. This is pipeline-level idempotency, layered on top of
    (not a replacement for) each stage's own idempotency.
    """
    if not force and is_processed(year, month, manifest_path):
        logger.info("%04d-%02d already processed, skipping", year, month)
        return {"skipped": True}

    raw_path = download_month(year, month)
    lf = pl.scan_parquet(raw_path)
    valid_df, rejected_df = apply_quality_rules(lf, year, month)
    write_rejected(rejected_df, year, month)
    cleaned = clean_trips(valid_df)
    demand = aggregate_hourly_demand(cleaned, year, month)
    write_curated(demand, year, month)

    info = {
        "processed_at": datetime.datetime.now(datetime.UTC).isoformat(),
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
    return [
        process_month(year, month, force=force, manifest_path=manifest_path)
        for year, month in months
    ]
