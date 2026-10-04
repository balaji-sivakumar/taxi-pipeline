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
- 🔜 Lesson 6: Identify nulls, invalid timestamps, impossible distances, negative fares — define explicit quality rules
- ⬜ Separate valid/rejected records (quarantine pattern)

## Phase 3 — Transformation and analytical storage
- ⬜ Query Parquet directly with DuckDB
- ⬜ Clean and transform trip records
- ⬜ Aggregate pickups by date/hour/zone
- ⬜ Write curated Parquet, understand partitioning

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
