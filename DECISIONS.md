# Decisions

Format: Decision / Context / Alternatives considered / Trade-off accepted.

## 1. Polars over pandas for ingestion through curated layers

**Context:** Need a dataframe library for reading Parquet, cleaning, and aggregating trip data across potentially many months.

**Alternatives considered:** pandas (ubiquitous, but eager/mostly single-threaded, row-index-centric); Polars (columnar-native, multi-threaded, stricter typing).

**Decision:** Polars for all stages through the curated layer. Convert to pandas only at the scikit-learn training boundary in Phase 6, since sklearn expects pandas/NumPy input.

**Trade-off accepted:** Smaller ecosystem and fewer Stack Overflow answers than pandas, in exchange for better performance on larger files and stricter type/null handling that surfaces data-quality issues instead of silently coercing them.

## 2. requests (streamed) over httpx for downloading source files

**Context:** Need to download single monthly TLC Parquet files (tens to hundreds of MB).

**Alternatives considered:** stdlib `urllib` (low-level, more boilerplate); `httpx` (adds async support we don't need).

**Decision:** `requests`, with `stream=True` to write to disk incrementally rather than buffering the whole file in memory.

**Trade-off accepted:** None meaningful at this scale — this is the simplest tool that fully satisfies the requirement.

## 3. Raw data files are gitignored, not committed

**Context:** The raw layer must preserve source files unchanged, but git is not designed for large binary files.

**Alternatives considered:** Commit raw Parquet files to git (rejected — bloats repo, git has no benefit for immutable binary blobs); use git-lfs (unnecessary complexity for a local-first, single-user learning project).

**Decision:** `data/raw/` is gitignored. "Preservation" is a filesystem/storage guarantee, not a version-control guarantee.

**Trade-off accepted:** No history of raw file changes via git — acceptable because raw files are never modified in place; a new download is a new file, not an edit.

## 4. January 2024 as the first month to ingest

**Context:** Need one concrete monthly file to build the first pipeline against. Very recent months can still be revised by TLC after initial publication (late-arriving corrections), which would make a "first pipeline run" output change later for reasons outside our code.

**Alternatives considered:** Most recent available month (rejected for lesson 1 of the pipeline — risks instability while we're still learning the basics); an arbitrary older month (works, but January is a clean calendar-aligned start).

**Decision:** `yellow_tripdata_2024-01.parquet` — confirmed via the live TLC/CloudFront source: 2,964,624 rows, 19 columns, 49,961,641 bytes, last modified March 2024 (so over a year settled, unlikely to be revised further).

**Trade-off accepted:** None meaningful — any stable past month would do; January 2024 is simply a concrete, verified choice.

## 5. Data contract recorded as code (`src/taxi_pipeline/schema.py`)

**Context:** We don't control the source schema, and TLC has changed it before (CSV→Parquet, column removals/renames historically). We want schema drift to fail loudly, not pass silently into curated data.

**Alternatives considered:** No explicit schema record, just re-inspect the file each time informally (rejected — not checkable by a test, not visible in code review); a full validation library/framework (rejected for now — premature before Phase 2's data-quality lessons).

**Decision:** Record the exact column names and Polars dtypes we observed from the real source file as a plain `dict[str, pl.DataType]` constant, guarded by a unit test. This is deliberately just a record for now; actually validating future files against it is Phase 2's job.

**Trade-off accepted:** This snapshot can go stale if TLC changes the schema and we forget to update it — acceptable because Phase 2 will build active validation against a live file, not just trust this constant blindly.

## 6. Idempotency check is size-verified, not just existence-based

**Context:** `download_month()` must be safely re-runnable. A naive `if path.exists(): skip` would treat a half-written or corrupted file from a crashed prior run as a successful prior run.

**Alternatives considered:** Existence-only check (rejected — not robust to corruption, as demonstrated by manually corrupting a downloaded file and confirming a naive check would have skipped it incorrectly); content hash/checksum comparison (more rigorous, but TLC doesn't publish per-file checksums, and size-matching against a live `HEAD` request is a cheap, available substitute); always re-download unconditionally (rejected — wastes bandwidth/time on every run, defeats the point of idempotency).

**Decision:** Before trusting an existing raw file, issue a `HEAD` request and compare declared `content-length` to the file's actual size on disk. Only skip if they match. Downloads are also written to a `.tmp` sibling and atomically `rename()`d into place only after the full download's size is verified — so a crash mid-download never leaves a file at the trusted final path at all.

**Trade-off accepted:** One extra `HEAD` request per run even when nothing needs downloading — negligible cost, worth the correctness guarantee. No retry logic yet on network failure — deliberately deferred to Phase 7 (Prefect); a failure here should raise loudly, not be silently swallowed.

## 7. Added pyarrow for physical Parquet metadata inspection

**Context:** Polars' native Rust Parquet reader doesn't expose row-group/column-chunk-level metadata (compression, encodings, embedded min/max/null-count statistics) through its public API — it's not designed for that. We needed this to understand the real physical structure of our raw file and to check whether row-group statistics (which enable predicate pushdown) are actually present.

**Alternatives considered:** DuckDB's `parquet_metadata()` table function (also capable, but rejected for now to keep DuckDB's introduction at Phase 3 as planned, rather than front-running it for a one-off inspection need); manually parsing the Parquet footer (rejected — reinventing a well-solved problem).

**Decision:** Added `pyarrow` as a new runtime dependency, used specifically for low-level metadata inspection via `pyarrow.parquet.ParquetFile(path).metadata`.

**Trade-off accepted:** One more dependency (34MB). Justified because it's the standard, purpose-built tool for this exact job, not a framework we're adopting wholesale.

**Finding from using it:** the real TLC file has zero columns with statistics written at all (`is_stats_set` is `False` everywhere) — confirmed by `created_by: parquet-cpp-arrow version 14.0.2` in the file's own metadata. Predicate pushdown via row-group statistics is therefore not available for these files as published; Phase 2's profiling work will need to compute null counts and value ranges itself rather than reading them for free from the file.

## 8. Schema contract validation implemented (`validate_schema`)

**Context:** Lesson 2 recorded the observed schema as a static constant and explicitly deferred "actually validating a file against it" to Phase 2.

**Decision:** `validate_schema(path)` in `src/taxi_pipeline/schema.py` compares a file's real schema (via `pl.scan_parquet(...).collect_schema()`) against `YELLOW_TRIPDATA_SCHEMA`, returning a list of human-readable differences (missing columns, unexpected new columns, type mismatches). An empty list means no drift.

**Trade-off accepted:** Only checks column names and dtypes, not row-level content — that's Phase 2's later data-quality/profiling work, a deliberately separate concern from schema validation.

## 9. One-pass profiling (`profile_parquet`) instead of per-column queries

**Context:** Need null counts and min/max ranges for every column to decide what Lesson 6's quality rules should actually check. Since the real file carries no embedded statistics (decision #7), every number has to be computed by scanning the data.

**Alternatives considered:** A separate `.select(...).collect()` per column/question (rejected — N separate full scans of a 47.6MB file instead of one); eagerly loading the whole file into memory first (rejected — unnecessary given Polars' lazy API can push all aggregations into a single scan).

**Decision:** `profile_parquet(path)` builds one list of Polars expressions (null count for every column, min/max for numeric and datetime columns only) and submits them as a single `.select(...).collect()` call, so Polars' query engine shares one pass over the file across every statistic.

**Trade-off accepted:** min/max is intentionally `None` for string/categorical columns (e.g. `store_and_fwd_flag`) — lexicographic min/max of a flag string isn't a meaningful question yet; revisit if a future column needs it.

**Real findings from running this against the actual raw file:** `tpep_pickup_datetime` has a minimum of `2002-12-31` inside a file that's supposed to be January 2024 only; `trip_distance` max is `312,722.3` (impossible); `fare_amount` goes as low as `-899.0`; several fee/surcharge columns go negative; `passenger_count` min is `0`; `RatecodeID` max is `99`, likely a sentinel/unknown code. These become the concrete candidates for Lesson 6's quality rules.

## 10. Quality rules implemented as the quarantine pattern, not silent drop/keep

**Context:** Lesson 5's profiling surfaced concrete anomalies (impossible distances, negative fares, a 2002 timestamp, zero-passenger trips). Need to decide what to do about them without either losing information (silent drop) or corrupting downstream aggregates (silent keep).

**Alternatives considered:** Silently filtering out bad rows (rejected — no audit trail, can't answer "how much data did we exclude and why"); silently keeping everything (rejected — a single `-$899` fare or 312,722-mile trip visibly distorts any aggregate); rejecting a row on its *first* failed rule only (rejected — loses diagnostic information when a row fails multiple rules for a related reason, e.g. a refund record failing both `fare_amount_non_negative` and `total_amount_non_negative` together).

**Decision:** `apply_quality_rules()` in `src/taxi_pipeline/quality.py` evaluates every named rule against every row and returns `(valid_df, rejected_df)`; rejected rows keep all original columns plus `failed_rules`, a list of every rule name that row failed (not just the first). `write_rejected()` persists rejected rows to a Hive-partitioned `data/rejected/year=/month=/` layer, mirroring the raw layer's own convention — gitignored for the same reason raw is (storage guarantee, not a version-control one).

**Specific rule thresholds and their reasoning:**
- `pickup_within_file_month`: pickup time must fall in `[year-month-01, next-month-01)`. Checks *pickup* only (not dropoff) because a trip starting Jan 31 and ending Feb 1 is normal, not an error.
- `dropoff_not_before_pickup`: a cross-column consistency check, not a range check — catches timestamp corruption a single-column bound can't.
- `trip_distance_plausible`: `0 ≤ distance ≤ 100` miles. Deliberately does *not* reject `0` — no evidence a zero-distance trip is wrong, only that very large ones are.
- `fare_amount_non_negative` / `total_amount_non_negative`: `≥ 0`. Negative values are refunds/corrections, not real trip charges.
- `passenger_count_plausible`: null **or** 1-9. Null is explicitly allowed through — profiling (Lesson 5) showed ~4.73% of rows are null across 5 unrelated columns together, almost certainly one vendor's feed not reporting them, which is *missing*, not *invalid*. Conflating the two would incorrectly quarantine ~140,000 legitimate trips.

**Result on the real January 2024 file:** 2,895,468 valid (97.67%), 69,156 rejected (2.33%). 35,384 of the rejected rows failed more than one rule simultaneously — mostly correlated `fare_amount`/`total_amount` negative-value pairs, consistent with being refund/correction records rather than independent random errors.

**Trade-off accepted:** Thresholds (100 miles, passenger count 1-9) are deliberate, documented judgment calls, not derived from an authoritative source — they could be revisited if evidence suggests otherwise. Naive (tz-unaware) datetimes are used intentionally in the month-boundary comparison, suppressing ruff's `DTZ001`, because TLC's own timestamp columns are tz-naive and undocumented as to timezone — asserting a timezone here would invent a fact, not fix one.

## 11. Added DuckDB for SQL-based querying/transformation, starting in Phase 3

**Context:** Phase 3's transformation work (joins against a zone lookup table in Phase 5, multi-file aggregation in Phase 4) is naturally SQL-shaped. Polars (Decision #1) remains fully capable of this too.

**Alternatives considered:** Staying entirely in Polars for aggregation (valid — Polars also supports Hive-partition-aware lazy scanning); introducing DuckDB earlier, e.g. for Lesson 4's metadata inspection (rejected then, to avoid front-running this planned introduction point).

**Decision:** Added `duckdb` as a dependency specifically for Phase 3's SQL-based transformation/aggregation work. It queries Parquet files in place — no `CREATE TABLE`/import step — and with `read_parquet(glob, hive_partitioning=true)`, it exposes our Lesson 3 directory-naming convention (`year=/month=`) as queryable columns even though they don't exist in the files' own schema at all.

**Important finding carried over from Lesson 4:** because the real file has zero embedded statistics, DuckDB's query planner has no real cardinality information either — `EXPLAIN` on `WHERE fare_amount > 100` estimated ~592,924 matching rows (a generic "20% selectivity" fallback guess); the actual count is 7,995, off by ~74x. This doesn't just disable row-group pruning (Lesson 4) — it also means the query optimizer's own planning decisions (e.g. join order in future multi-table queries) are working from a bad estimate, not just a missed optimization.

**Trade-off accepted:** A second SQL-capable tool alongside Polars in the stack — justified because it was the course's planned tool for this role and because its Hive-partition-aware multi-file querying is a direct fit for Phase 4's multi-month shape. DuckDB is a convenience/ergonomics choice for this phase, not something data engineering is inherently tied to — everything built in Phases 1-2 (contracts, idempotency, validation, quarantine) remains valid regardless of which query engine does the aggregation.

## 12. clean_trips() uses DuckDB internally, but keeps a Polars-in/Polars-out contract

**Context:** Decision #1 committed to Polars through the curated layer; Decision #11 introduced DuckDB for Phase 3's SQL-shaped work. `clean_trips()` (renaming columns, deriving `pickup_hour`) is exactly SQL-shaped, but needs to compose with `quality.py`'s Polars output on one side and whatever Lesson 9 does on the other.

**Decision:** `clean_trips(valid_df: pl.DataFrame) -> pl.DataFrame` takes a Polars DataFrame and returns one — DuckDB only appears *inside* the function body, via `duckdb.sql(...).pl()`. Verified DuckDB can query a Polars DataFrame directly by Python variable name (`FROM valid_df` with no load/registration step) before relying on it.

**Finding:** DuckDB's native timestamp resolution is microseconds. A column selected unchanged (`pickup_datetime`) keeps its original `ns` precision from the source file, but a *derived* column (`pickup_hour`, via `date_trunc`) comes back as `us`. This only affects computed columns, not passthrough ones — confirmed by checking both columns' dtypes after the same call. Harmless for hourly aggregation, but a real, silent type change worth knowing rather than discovering by accident.

**Curated-layer column selection, a deliberate narrowing:** kept `pickup_datetime`, `pickup_hour`, `pickup_location_id`, `dropoff_location_id`, `trip_distance`, `passenger_count`, `fare_amount`, `total_amount`. Dropped fare-breakdown detail columns (`mta_tax`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `Airport_fee`, `extra`), `VendorID`, `RatecodeID`, `store_and_fwd_flag`, and `payment_type` — none of these are needed for hourly pickup-demand forecasting (the project's actual business problem from Lesson 0), and raw already preserves them in full if a future need arises.

**Verified against the real file:** row count in equals row count out (2,895,468 → 2,895,468, nothing silently dropped), and hour-bucketing is correct at both a within-day boundary (`10:59:59`→hour 10, `11:00:00`→hour 11) and a day boundary (`23:59:59` stays in hour 23, doesn't spill to the next day).

## 13. Dense hour×zone grid, built in Polars not DuckDB, with explicit zero-fill

**Context:** `aggregate_hourly_demand()` needs to turn trip-level rows into hourly pickup counts per zone — the actual curated signal this project exists to produce. A plain `GROUP BY` only emits rows for combinations that occurred in the data.

**Alternatives considered:** A naive `group_by(pickup_hour, pickup_location_id).agg(count())` (rejected — proven to silently omit 116,051 of 193,440 real hour-zone combinations on the actual January 2024 data, exactly because zero-trip combinations produce no row to group); doing this step in DuckDB, continuing Lesson 8's tool (rejected for *this specific step* — the cross join, group-by, and left join are equally natural in Polars' own API, and staying in Polars avoids re-introducing the microsecond-precision downcast found in Lesson 8 on a join-critical column).

**Decision:** Build the complete grid explicitly: every hour in the month (`pl.datetime_range`) cross-joined with every zone *observed anywhere in the month's cleaned data* (not the official ~265-zone TLC list, which isn't loaded yet — documented as a real, current limitation), left-joined against actual counts, with `fill_null(0)`.

**Verified against the real file:** grid shape is exactly `744 hours × 260 observed zones = 193,440` rows; `pickup_count` sums to exactly `2,895,468` (the valid trip count, nothing double-counted or dropped); confirmed the join's `pickup_hour` dtype matches on both sides (`datetime[μs]` from Lesson 8's DuckDB-derived column vs. `pl.datetime_range`'s default) before trusting any result — a dtype mismatch here would have silently produced all-null counts instead of an error.

**Refactor alongside this lesson:** extracted `month_bounds(year, month)` out of `quality.py` into a new `dates.py`, since this lesson needed the identical calculation — the second real use case, not a speculative one, is what justified the extraction (see `CLAUDE.md`'s "refactor only when the need becomes visible").

**Trade-off accepted:** "Complete" here means complete relative to zones that had *some* activity in the month, not the true universe of all NYC taxi zones. A zone with zero pickups for an entire month is still absent from the grid. Revisit once the official TLC zone lookup table is loaded (likely Phase 5).

## 14. Curated partitioned by year=/month=, same granularity as raw — and a different idempotency model

**Context:** `aggregate_hourly_demand()`'s output (Lesson 9) needs a persisted home. The tempting alternative is partitioning by `pickup_location_id` (zone), since Phase 6 will eventually want per-zone time series.

**Alternatives considered:** Partition by zone (rejected — one month's aggregation touches all 260 zones in a single run; writing by zone would mean 260 small files per month for no pruning benefit not already achievable via column pruning + a `WHERE` filter within one month-sized file); partition by year only (rejected — too coarse once multiple months accumulate, poor pruning).

**Decision:** `data/curated/year=/month=/`, matching raw's granularity exactly — because partitioning granularity should match the unit the pipeline actually processes in (ingest, validate, transform, and aggregate all already operate one month at a time), which matters directly for Phase 4's incremental processing.

**A different idempotency model than `download_month()` (Lesson 3), deliberately:** `download_month()` is idempotent via a *skip-if-correct* check, because downloading is expensive and external. `write_curated()` has no skip check at all — it **always overwrites**, because the aggregation is a deterministic, cheap-to-recompute function of raw data + code; re-running should replace stale output with whatever the current logic produces, not preserve a possibly-outdated prior result. This is idempotency in the more fundamental sense (repeating the operation doesn't change the correctness of the outcome) without needing a skip optimization on top of it. The atomic temp-file + rename pattern *does* carry over from Lesson 3, for the same reason: a crash mid-write shouldn't leave a corrupted file at the trusted path.

**Verified against the real file:** 47.6MB raw → 114KB curated (~400x reduction) for January 2024 — the actual size of the signal Phase 6 will train on. Confirmed via DuckDB's Hive-partition glob (same mechanism as Lesson 7) that `total_pickups` across the curated layer sums to exactly `2,895,468`, matching the valid trip count end-to-end. Confirmed overwrite-not-merge directly: writing a 1-row result over a 193,440-row prior file yields exactly 1 row back, with no leftover `.tmp` file.

## 15. Pipeline-level idempotency via a JSON manifest, layered on top of stage-level idempotency

**Context:** Every individual stage is already idempotent (Lessons 3, 10), but nothing tracked whether a given month's *entire* pipeline — download through curated — had actually finished. Needed to process multiple months (Phase 4's actual scope) without redoing already-finished work on every rerun.

**Alternatives considered:** A Parquet manifest, for tool consistency with the rest of the project (rejected — a manifest is a small, constantly read-modify-written *control* file, not bulk analytical data; Parquet's columnar/row-group model is the wrong fit for something rewritten in full on every single run); inferring "already processed" by checking whether the curated file exists (rejected — conflates "a file is present" with "the pipeline that produced it actually succeeded," the same category of mistake Lesson 3 rejected for raw-file idempotency).

**Decision:** `src/taxi_pipeline/manifest.py` — a small JSON file (`data/manifest.json`) keyed by `"YYYY-MM"`, holding per-month metadata (`processed_at`, `valid_rows`, `rejected_rows`, `curated_rows`). `process_month()`/`process_months()` in `src/taxi_pipeline/pipeline.py` check the manifest first and skip entirely if already recorded, unless `force=True`. Every function accepts an overridable path parameter (`manifest_path`), consistent with every other I/O function in this project (`raw_dir`, `rejected_dir`, `curated_dir`).

**`data/manifest.json` is gitignored, deliberately, unlike a first instinct might suggest:** its claims ("January is processed") are only meaningful relative to `data/raw/` and `data/curated/` actually existing locally — both already gitignored. Committing the manifest alone would let a fresh clone show months as "processed" with no underlying data to back that up — a control file lying about state it doesn't actually reflect.

**A real contrast in timestamp handling, worth noting explicitly:** `processed_at` uses a proper **timezone-aware** UTC timestamp (`datetime.now(datetime.UTC)`), unlike the deliberately-naive datetimes used throughout `quality.py`/`aggregate.py` for matching TLC's own undocumented-timezone columns. There's no external naive contract to match here — this is our own operational metadata, so there's no reason not to do it correctly.

**Verified against the real pipeline:** processed January (raw already present, skipped download) and February (fresh download) for the first time; reran the identical call and confirmed both were skipped with zero filesystem/network activity; confirmed `force=True` bypasses the manifest-level skip while `download_month()`'s own stage-level skip still independently applies underneath it (download stayed skipped since the file was genuinely unchanged — only the compute stages re-ran, consistent with Lesson 10's "cheap to recompute" design). February's grid came out to exactly `29 days × 24 hours × 254 observed zones = 176,784` rows, confirming the leap-year math and a different (smaller) observed-zone count than January's 260 — consistent with Decision #13's documented "observed zones only" limitation.

**Explicitly a stepping stone, not a permanent design:** this hand-rolled manifest is what motivates Phase 7's orchestrator — Prefect will provide proper run history/state tracking, which this lesson's manual bookkeeping should make legible as a real need rather than an arbitrary tool requirement.

## 16. Deduplication investigated and deliberately not implemented — the data doesn't support it safely

**Context:** Phase 4 explicitly calls for "deduplication." Before writing a dedup step, checked the real January 2024 data for duplicates two ways: exact full-row match, and a plausible business key (`VendorID`, pickup time, dropoff time, PU zone, DO zone).

**Findings:** Zero exact full-row duplicates. The business key flagged 62,218 "duplicate" rows — but inspecting them showed the overwhelming majority (62,202 of 62,218) are legitimate **charge/refund reversal pairs**: identical trip details with the fare's sign flipped (e.g. `-6.5`/`+6.5`), representing two distinct real events (a charge and its correction), not one trip recorded twice. These are already resolved as a side effect of Lesson 6's unrelated `fare_amount_non_negative` rule, which rejects the negative half of each pair. The remaining 16 rows have the *same* business key but *different* `trip_distance`/`fare_amount`/`passenger_count` — almost certainly genuinely distinct trips that coincidentally share a pickup zone, dropoff zone, and to-the-second timestamp, which is plausible in a busy zone during a busy minute.

**Decision:** Do not implement a deduplication step. TLC's schema has no trip-level unique identifier at all, and the most plausible available business key produces false positives — dropping rows that match it would silently delete real trips, and would do so disproportionately in the busiest zone/time combinations, introducing a systematic undercount exactly where it's least visible and most damaging to the demand-forecasting use case.

**Alternatives considered:** Deduplicating on the business key anyway (rejected — proven to be wrong, not just unnecessary, by direct inspection of the 16 surviving rows); deduplicating only the exact-match case (rejected as pointless — zero exact duplicates exist in the real data, so this would be dead code with nothing to protect against).

**Trade-off accepted:** If TLC's data ever does contain genuine accidental duplicates (not demonstrated here), they would currently pass through uncaught. Revisit if a future month's profiling (Lesson 5's tool) surfaces evidence of the exact-match case actually occurring.

## 17. Source-change detection in process_month(), trading away Lesson 11's zero-cost skip

**Context:** `process_month()`'s manifest check (Lesson 11) only asked "is this month recorded as processed" — it had no way to detect that TLC republished a corrected file for an already-processed month (a real instance of "late-arriving data").

**Decision:** `download_month()` now always runs first, every time — including for months we expect to skip — so its existing `HEAD`-based size check (Lesson 3) can detect a changed source file. `process_month()` then compares the current raw file's size against `raw_file_size` recorded in the manifest; only skips the compute stages if they match. A mismatch is logged as a warning and triggers full reprocessing, without needing `force=True`.

**Alternatives considered:** Checking the manifest key first and only calling `download_month()` if not already processed (the original Lesson 11 order — rejected now, since it can never detect a source change for a month already marked done); hashing the file's content instead of using size (more rigorous, but we don't have per-file checksums from TLC either, and size was already the established integrity signal from Decision #6 — reusing it keeps the project's reasoning consistent rather than introducing a second verification method).

**Trade-off accepted, explicitly reversing part of Lesson 11's result:** a "skip" is no longer zero network activity — it now costs one `HEAD` request per month, every run. This is a deliberate correctness-over-cost trade: a `HEAD` request costs milliseconds against the seconds-to-minutes of the compute stages it allows skipping, and detecting real source corrections is worth that cost. Also demonstrated the practical consequence directly: existing manifest entries from Lesson 11 (written before `raw_file_size` existed) were correctly treated as needing reprocessing — `previous.get("raw_file_size")` returns `None`, which never matches a real size, so an old-format manifest entry safely triggers one necessary reprocessing rather than crashing or silently trusting stale data.

## 18. Backfill resilience: process_months() isolates per-month failures instead of aborting

**Context:** A real backfill reprocesses many historical months at once (`process_months(..., force=True)`). One month's failure (a network error, a malformed file, anything) shouldn't block correcting every other month in the batch.

**Decision:** `process_months()` wraps each `process_month()` call in its own `try`/`except Exception`, logging the failure clearly (`logger.error`) and recording it as `{"error": str(exc)}` in that month's result slot, then continuing to the next month. Verified directly: mixed a genuinely nonexistent month (`2030-06`, a real `403` from CloudFront) in between two valid months and confirmed both valid months completed successfully while the bad month's failure was captured distinctly, not swallowed and not fatal to the batch.

**Alternatives considered:** Letting any exception propagate and abort the whole batch (rejected — exactly the failure mode a real backfill over months or years of data can't tolerate); catching a narrower exception type (rejected — defeats the purpose, since a backfill driver can't know in advance whether a bad month will fail with a network error, a Polars computation error, or something else entirely; this is one of the legitimate cases for catching `Exception` broadly, suppressed explicitly with `# noqa: BLE001` and a reason, same pattern as earlier `noqa` uses in this project).

**Trade-off accepted:** Failures are reported in the return value, not raised — a caller who doesn't inspect results for `"error"` keys could miss a failure silently. Acceptable for now since this is still a manually-invoked function, not yet wired into automated scheduling; Phase 7's orchestrator will have its own, more visible failure-surfacing (retries, alerting) that this caveat should motivate.

## 19. validate_schema() wired into process_month(); profile_parquet() deliberately left out

**Context:** Both tools were built (Lessons 4, 5) but neither was ever called from `process_month()`, left as an open question through Phase 3 and most of Phase 4.

**Decision:** `validate_schema()` now runs on every real processing attempt (after the skip check, before quality rules), raising `RuntimeError` with the specific column-level differences if the source schema has drifted — turning a would-be confusing downstream failure (e.g. quality rules referencing a column that no longer exists) into one clear, specific error. `profile_parquet()` stays un-wired, permanently: it's a diagnostic tool for a human deciding whether quality rules need updating (Decision on the validate-vs-profile distinction, discussed directly in conversation), with no automated action to take on its output — wiring it in would just compute statistics and discard them on every run for no benefit.

**A clean composition, not a new mechanism:** the new `RuntimeError` on schema drift is automatically caught and isolated by Lesson 12's `process_months()` broad exception handling, with zero changes needed there — a concrete sign the backfill-resilience abstraction from Lesson 12 was built generally enough to absorb a new failure mode it was never specifically designed around.

**A real gap surfaced by writing the first non-mocked integration test:** `process_month()` had no way to redirect raw/rejected/curated output to a test directory — only `manifest_path` got that treatment in Lesson 11. Added `raw_dir`/`rejected_dir`/`curated_dir` parameters, threaded through to `download_month()`/`write_rejected()`/`write_curated()`, consistent with every other I/O function in this project. This is exactly what integration tests are for beyond catching logic bugs: writing one surfaced a missing seam in the code's own configurability that no mocked unit test ever would have, since mocked tests never actually call the real I/O functions in the first place.

**Unit tests vs. integration tests, concretely distinguished in this codebase:** `test_pipeline.py` mocks every stage (including the now-added `validate_schema`) to test `process_month()`'s own control flow (skip logic, force, source-change detection) in isolation and fast. `test_integration.py` is new and mocks only `download_month()` — every other function (`validate_schema`, `apply_quality_rules`, `write_rejected`, `clean_trips`, `aggregate_hourly_demand`, `write_curated`) runs for real against a small synthetic Parquet file, proving the actual stages compose correctly together. Verified directly: a quarantined row's `failed_rules` persisted correctly to a real rejected file, a specific hour/zone's `pickup_count` came out correct in a real curated file, and a schema-drift scenario (a missing column) raised a `RuntimeError` naming `trip_distance` specifically, not a generic failure.

**Gap found after declaring this lesson done, and fixed before actually calling Phase 4 complete:** every integration test above called `process_month()` (singular) — none exercised `process_months()` with real, non-mocked stages. The only test of `process_months()` at all (`test_process_months_continues_past_one_failure`, Lesson 12) mocks every stage, so it only proved the *control flow* of continuing past a failure, never that running several real months together actually composes correctly. Added two tests: `test_process_months_runs_multiple_real_months_independently` (two distinct real synthetic months, confirming each month's curated output reflects only its own data — no cross-contamination between months processed in the same call) and `test_process_months_preserves_completed_real_months_when_a_later_one_fails` (a real failure for one month in the middle of a three-month real run, confirming the failed month leaves no trace at all — no manifest entry, no curated directory — while the two real successes on either side complete normally).

**Worth stating precisely, since it was asked directly: `process_months()` is strictly serial.** It's a plain `for year, month in months:` loop — one month fully completes or fails before the next one starts. There is no parallelism anywhere at the month-orchestration level in this project. (Individual stages use multi-threading *within* one month's computation via Polars/DuckDB — unrelated to whether multiple months run concurrently, which they don't.)

**Phase 4 is complete.**

## 20. Trend analysis lives outside the automated pipeline, same reasoning as profiling

**Context:** Phase 5 begins actually using the curated data (Phases 1-4 only ever built and verified it). First question: where does exploratory analysis code live relative to `pipeline.py`?

**Decision:** `src/taxi_pipeline/analysis.py` is never imported by `pipeline.py` — same reasoning as `profile_parquet()` (Decision in Lesson 13): this is a human-driven exploration tool with no automated action to take on its output, not a pipeline stage. `load_curated()` reads the curated layer across all available months via a glob, independent of any single month's processing run.

**A genuinely new aggregation level, distinct from anything in the pipeline:** the pipeline deliberately keeps `(pickup_hour, pickup_location_id)` granular (Lesson 9's whole point was not losing that). `hourly_profile()`/`day_of_week_profile()` deliberately collapse it — first summing across zones (one citywide total per actual date+hour), then **averaging** (not summing) across dates. The mean at the second step isn't a style choice: the dataset's date range doesn't contain an equal number of each weekday, so summing would bias the day-of-week comparison toward whichever weekday happens to occur more often in Jan+Feb 2024. Tested directly: a weekday appearing twice with identical per-occurrence magnitude as another appearing once must produce identical averages, not an inflated one.

**Real findings from the actual Jan+Feb 2024 curated data (60 days):** a single citywide demand peak at **6pm** (7,123 avg pickups), not the two-peak commuter pattern subway ridership typically shows — consistent with taxi demand being driven more by general daytime/evening activity than fixed commute hours. Day-of-week: **Thursday highest** (110,777), **Monday lowest** (81,414), with Saturday close behind Thursday (105,703) — not a simple weekday/weekend split.

**Matplotlib introduced for the first time** (planned in the original tech stack, unused until now) — `plot_hourly_profile()`/`plot_day_of_week_profile()` are tested for "runs correctly and produces a real non-empty file," since asserting on pixel content isn't meaningful; the two real charts were visually inspected directly against the numeric tables they're built from, not trusted blindly.

**Trade-off accepted:** Generated chart PNGs are not committed to the repo — they're regenerable from code + data at any time, and would go stale the moment more months are added, same reasoning as not committing `data/curated/` itself.
