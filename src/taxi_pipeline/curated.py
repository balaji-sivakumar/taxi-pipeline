from pathlib import Path

import polars as pl

CURATED_DIR = Path("data/curated")


def curated_path_for(year: int, month: int, curated_dir: Path = CURATED_DIR) -> Path:
    return (
        curated_dir
        / f"year={year:04d}"
        / f"month={month:02d}"
        / f"hourly_demand_{year:04d}-{month:02d}.parquet"
    )


def write_curated(
    demand_df: pl.DataFrame, year: int, month: int, curated_dir: Path = CURATED_DIR
) -> Path:
    """Always overwrites: curated is a deterministic function of raw + code, so re-running
    should replace prior output with whatever the current logic produces, not skip the work."""
    target = curated_path_for(year, month, curated_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_suffix(target.suffix + ".tmp")
    demand_df.write_parquet(tmp_path)
    tmp_path.rename(target)
    return target
