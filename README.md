# NYC Taxi Demand Forecasting

A hands-on data-engineering course built around a real pipeline: forecast next-day hourly Yellow Taxi pickup demand per zone, using NYC TLC's public monthly trip-record Parquet files.

This is a learning project, structured as sequential lessons (see `COURSE_PLAN.md`). Decisions and their trade-offs are logged in `DECISIONS.md`; concepts learned are logged in `LEARNING_LOG.md`.

## Setup

```bash
uv sync          # installs pinned dependencies from uv.lock into .venv
uv run pytest    # run tests
uv run ruff check .      # lint
uv run ruff format .     # format
```

## Project layout

```
data/raw/            # immutable source files as downloaded, gitignored
src/taxi_pipeline/   # pipeline code
tests/                # tests
```

## Status

Phase 1 (Foundation & ingestion) in progress. See `COURSE_PLAN.md` for details.
