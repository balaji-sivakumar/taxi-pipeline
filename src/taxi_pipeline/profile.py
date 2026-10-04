from pathlib import Path

import polars as pl


def profile_parquet(path: Path) -> list[dict]:
    """One-pass profile: null count/fraction for every column, min/max for orderable columns.

    Returns one dict per column: column, dtype, null_count, null_fraction, min, max.
    min/max are None for columns where ordering isn't meaningful (e.g. strings).
    """
    lf = pl.scan_parquet(path)
    schema = lf.collect_schema()

    exprs = [pl.len().alias("__total_rows__")]
    for name, dtype in schema.items():
        exprs.append(pl.col(name).null_count().alias(f"{name}__null_count"))
        if dtype.is_numeric() or dtype.is_temporal():
            exprs.append(pl.col(name).min().alias(f"{name}__min"))
            exprs.append(pl.col(name).max().alias(f"{name}__max"))

    wide = lf.select(exprs).collect()
    total_rows = wide["__total_rows__"][0]

    profiles = []
    for name, dtype in schema.items():
        null_count = wide[f"{name}__null_count"][0]
        profiles.append(
            {
                "column": name,
                "dtype": str(dtype),
                "null_count": null_count,
                "null_fraction": (null_count / total_rows) if total_rows else None,
                "min": wide[f"{name}__min"][0]
                if f"{name}__min" in wide.columns
                else None,
                "max": wide[f"{name}__max"][0]
                if f"{name}__max" in wide.columns
                else None,
            }
        )

    return profiles
