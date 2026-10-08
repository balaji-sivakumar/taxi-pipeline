# Course Plan

Status legend: ✅ done · 🔜 next · ⬜ not started

## Phase 1 — Foundation & ingestion
- ✅ Lesson 0: business problem, pipeline overview, app-dev vs data-eng, curriculum approval
- ✅ Lesson 1: reproducible environment (`uv`), Polars vs pandas decision, requests for downloads, project skeleton, environment smoke test. Comprehension check: 4/4 correct (Polars/Parquet fit, transitive-graph lock, --locked vs --frozen, raw-as-evidence).
- ✅ Lesson 2: explored real TLC/CloudFront source, chose yellow_tripdata_2024-01.parquet, inspected schema/nulls/size with Polars, recorded data contract as code (`schema.py`), fixed packaging gap, hit and resolved a real TLS-interception network issue. Comprehension check: 3/3 correct.
- ✅ Lesson 3: wrote `ingest.py` (`download_month`) with size-verified idempotency and atomic temp-file writes into a Hive-partitioned raw layer; demonstrated fresh download, idempotent skip, and recovery from a deliberately corrupted file; added network-free mocked tests. Comprehension check: 3/3 correct.

## Phase 2 — Data understanding and quality
- ✅ Lesson 4: inspected physical Parquet metadata via pyarrow (row groups, column chunks, statistics) — found the real file has zero embedded statistics; built `validate_schema()` turning Lesson 2's static contract into an active, tested check against schema drift. Comprehension check: 3/3 correct.
- ✅ Lesson 5: built `profile_parquet()` (one-pass null count/fraction + min/max per column) and ran it against the real raw file — surfaced a 2002 pickup timestamp, a 312,722-mile trip, fares to -$899, zero-passenger trips, and a RatecodeID=99 sentinel. Comprehension check: 3/3 correct.
- ✅ Lesson 6: defined 6 named, documented quality rules (`build_quality_rules`), implemented the quarantine pattern (`apply_quality_rules` + `write_rejected` into `data/rejected/`) — real result 97.67% valid / 2.33% rejected, 35,384 rows failing multiple rules at once. Caught and correctly suppressed a real ruff DTZ001 finding (naive datetimes are intentional, matching TLC's own undocumented-timezone columns). Comprehension check: 3/3 correct.

## Phase 3 — Transformation and analytical storage
- ✅ Lesson 7: queried the raw Parquet file directly with DuckDB (no load step), confirmed column-projection pushdown via EXPLAIN, found DuckDB's cardinality estimate off by ~74x due to Lesson 4's missing statistics, and exposed Hive partition columns (year=/month=) via `read_parquet(glob, hive_partitioning=true)`. Comprehension check: 3/3 correct.
- ✅ Lesson 8: built `clean_trips()` — DuckDB internally (renaming columns to curated names, deriving `pickup_hour` via `date_trunc`), Polars in/out at the boundary. Verified against the real file: row count preserved exactly, hour-bucketing correct at hour and day boundaries. Found DuckDB silently downcasts derived (not passthrough) timestamp columns to microsecond precision. Comprehension check: 3/3 correct.
- ✅ Lesson 9: built `aggregate_hourly_demand()` — a dense hour×zone grid (pure Polars, not DuckDB) with explicit zero-fill. Proved a naive GROUP BY would have silently missed 116,051 of 193,440 real hour-zone combinations (60%). Verified grid shape (744×260) and count conservation (sums to 2,895,468) against the real file. Extracted `month_bounds()` from `quality.py` on its second real use. Comprehension check: 3/3 correct.
- 🔜 Lesson 10: Write curated Parquet, understand partitioning

## Phase 4 — Multi-month reliable pipeline
- ⬜ Process multiple monthly files, track processed inputs
- ⬜ Incremental loads, deduplication, backfills
- ⬜ Automated tests

## Phase 5 — Trend analysis
- ⬜ Hourly/daily/weekly demand analysis, zone comparisons, visualizations

## Phase 6 — Prediction
- ⬜ Define prediction target, baseline, calendar/lag features (leakage-safe)
- ⬜ Train and evaluate model, generate next-day predictions

## Phase 7 — Orchestration and operations
- ⬜ Prefect tasks/flows, retries, logging, scheduled/backfill runs, monitoring

## Phase 8 — Serving and architecture
- ⬜ API/dashboard for curated demand + predictions
- ⬜ Architecture docs, containerization

## Phase 9 — Advanced data engineering
- ⬜ Simulate taxi events, batch vs streaming, event vs processing time, local vs cloud design
