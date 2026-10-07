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
