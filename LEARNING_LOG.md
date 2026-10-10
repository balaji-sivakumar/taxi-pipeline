# Learning Log

## Lesson 0 — Foundation
- Data engineering defends against inputs it doesn't control (external schemas, external sources); application engineering usually controls its own inputs via its own API contracts.
- Raw/staging/curated layering exists because curated data must always be rebuildable from raw, but raw can never be reconstructed from curated.
- Idempotency and backfill mean something different here than in app dev: re-running a pipeline on the same input must not duplicate data, and reprocessing historical data after a bug fix must produce consistent results.

## Lesson 1 — Reproducible environments
- Pipelines get re-run long after they were written (backfills, scheduled reruns), so environment drift is a correctness risk, not just an inconvenience — `uv.lock` pins exact resolved versions (including transitive deps) to prevent it.
- Chose Polars over pandas for ingestion through the curated layer: columnar-native (matches Parquet), multi-threaded, stricter typing surfaces data-quality problems instead of hiding them. Converting to pandas is deferred to the sklearn boundary (Phase 6) — data engineering and data science stages can use different tools as long as the handoff is explicit.
- Chose `requests` (streamed to disk) over `httpx` for downloading source files — simplest tool that satisfies the requirement; no async need here.
- Git is for code and lockfiles, not large binary data — raw Parquet files are gitignored; "preservation" is a filesystem guarantee, not a version-control guarantee.
- First test added was an environment smoke test (Python version + dependency imports), not pipeline logic — catches environment problems before they masquerade as data problems three stages later.

## Follow-up — pyproject.toml / uv.lock / .venv are three separate states
- `pyproject.toml` (intent/constraints) and `uv.lock` (exact resolution) are both committed to git and update together when you run `uv add`. `.venv` (what's actually installed) is a derived, gitignored artifact that only updates when a `uv` command explicitly materializes it.
- `git pull` can update the first two instantly without touching `.venv` at all — so a teammate's environment can briefly disagree with the lock file right after pulling. Verified this by removing a package from `.venv` only (`uv pip uninstall`) while leaving `uv.lock` untouched — plain `python` then raised `ModuleNotFoundError`, while `uv run python` auto-detected the mismatch and healed it before executing.
- `--frozen` installs from the lock without checking it's still consistent with `pyproject.toml`. `--locked` actually validates consistency and fails loudly on drift (tested earlier by introducing an impossible version constraint). CI should use `--locked`; a production image build can use `--frozen` once CI already proved the lock is valid.
- CI and Docker builds don't have the "stale local `.venv`" problem at all — they always start from a fresh checkout with no pre-existing environment, so there's nothing to drift.

## Lesson 2 — Source systems and data contracts
- Verified the real TLC source instead of trusting memory: files are served from `d37ci6vzurychx.cloudfront.net` (CloudFront/S3), not nyc.gov itself; URL pattern is `yellow_tripdata_YYYY-MM.parquet`.
- Chose January 2024 (not the newest available month) specifically because recent months can still be revised by TLC after publication — an instance of "late-arriving data" we'll formalize in Phase 4. Confirmed via headers: 49,961,641 bytes, last modified March 2024.
- Real data observations: ~3M rows / 19 columns compressed into 47.6MB (columnar compression payoff); `Airport_fee` is inconsistently capitalized vs. other columns; 5 unrelated columns share an identical null count (140,162), suggesting a structural cause (e.g. a vendor/trip type not reporting those fields) rather than random missingness — a lead for Phase 2.
- `pl.scan_parquet(...).collect_schema()` and row-count-via-metadata are cheap because Parquet stores this in file footer metadata — no full data read required. Same underlying idea as the HTTP `accept-ranges: bytes` we saw on the CloudFront response.
- Encoded the observed schema as an explicit, testable constant (`src/taxi_pipeline/schema.py`) rather than relying on informal re-inspection — this is what "data contract as code" means in practice: an assumption a test can fail against.
- Real failure hit (not staged): a Fortinet network appliance was intercepting/re-signing HTTPS to CloudFront, causing SSL verification failures in both curl and Python requests. Diagnosed via `openssl s_client` showing the cert issuer was Fortinet, not Amazon. Fixed by switching networks — not a code bug, and never fixable by disabling TLS verification.
- Packaging gap: `src/taxi_pipeline` wasn't actually importable until `[build-system]` (hatchling) was added to `pyproject.toml` — a src-layout package needs explicit build-system config, it isn't automatic just from directory structure.

## Lesson 3 — Idempotent ingestion
- Idempotency for ingestion means more than "file exists" — it means "file exists in the state a successful prior run would have left it." Verified this concretely: corrupted an existing raw file down to 26 bytes, and `download_month()` correctly detected the size mismatch against a live `HEAD` request and re-downloaded, rather than trusting the corrupted file.
- Atomic writes (temp file + `rename()` only after size verification) are what make the above check trustworthy — without it, a crash mid-download could itself leave a corrupted file sitting at the path we later trust.
- Introduced Hive-style raw partitioning (`year=2024/month=01/...`) without yet justifying the trade-offs — that's Phase 3's job; for now it's just the convention we're committing to.
- Tests for ingestion mock `requests.head`/`requests.get` entirely — a real network call in a test suite can fail for reasons that have nothing to do with code correctness (we proved this ourselves with the Fortinet interception in Lesson 2).
- Deliberately no retry logic on network failure yet — a bare exception now is more honest than silently swallowing a failure; real retry/scheduling behavior is Phase 7's job (Prefect).

## Lesson 4 — Parquet physical metadata and schema validation
- A Parquet file is physically organized into row groups, each containing one independently-compressed column chunk per column; the footer can store per-chunk min/max/null-count statistics that let query engines skip whole row groups without reading them (predicate pushdown).
- Real finding, not staged: TLC's published files have **no statistics written at all** (`is_stats_set` is `False` for every column), per `pyarrow.parquet.ParquetFile(path).metadata`. The format allows statistics; this writer simply didn't produce them. Means Phase 2 profiling must compute null counts/ranges itself rather than reading them for free.
- Closed the loop from Lesson 2: `validate_schema()` now actively compares a file's real schema against our recorded `YELLOW_TRIPDATA_SCHEMA` contract and reports differences (missing column, new column, dtype mismatch) instead of just assuming the recorded contract still holds.
- Added `pyarrow` as a new dependency specifically for metadata inspection — deliberately did not reach for DuckDB yet, even though it could do this too, to keep DuckDB's introduction at the point the course planned (Phase 3).

## Lesson 5 — Profiling the data
- Profiling is a distinct step from schema validation: validation asks "is the structure right" (columns/types); profiling asks "what does the content actually look like" (null rates, ranges) — without yet judging anything as valid or invalid. Profiling is what tells you what to even write quality rules about.
- `profile_parquet()` computes null count/fraction for every column and min/max for numeric/temporal columns in a single Polars `.select(...).collect()` call, so the engine shares one scan of the file across every statistic instead of re-scanning per question.
- Real, unstaged findings from the actual raw file: `tpep_pickup_datetime` min is `2002-12-31` (inside a file that should only contain January 2024 trips); `trip_distance` max is `312,722.3` miles; `fare_amount` min is `-899.0`; several fee columns go negative; `passenger_count` min is `0`; `RatecodeID` max is `99`, likely a sentinel/unknown value rather than a real code. These are now the concrete candidates for Lesson 6's quality rules.
- min/max is deliberately left `None` for string/categorical columns (e.g. `store_and_fwd_flag`) — a scope decision (lexicographic ordering isn't a meaningful question for that column yet), not a technical limitation.

## Lesson 6 — Quality rules and the quarantine pattern
- Quarantine beats both silent-drop (loses auditability) and silent-keep (corrupts downstream aggregates): rejected rows are preserved in their own Hive-partitioned layer (`data/rejected/`), tagged with every rule they violated, not just the first.
- A rule set is a set of deliberate, documented decisions, not a universal truth — each threshold (100-mile cap, passenger count 1-9) is traceable to a specific profiling finding from Lesson 5, recorded with reasoning in `DECISIONS.md`.
- Missing is not the same as invalid: `passenger_count` null is allowed through (structural, one vendor's feed, per Lesson 5's co-occurring-nulls finding) while `passenger_count = 0` (present but implausible) is rejected. Conflating the two would have wrongly quarantined ~140,000 legitimate trips.
- Real result on the actual file: 97.67% valid / 2.33% rejected; 35,384 rejected rows failed more than one rule at once, mostly correlated negative fare/total pairs — evidence they're refund/correction records, not independent random errors.
- Caught a real `ruff` finding (`DTZ001`, naive datetime) and correctly did *not* blindly fix it — TLC's own timestamp columns are tz-naive and the timezone is undocumented, so asserting one would invent a fact. Suppressed with an explanation instead of silencing the rule project-wide.

## Lesson 7 — Querying Parquet directly with DuckDB
- DuckDB queries a Parquet file in place via a path string in SQL — no `CREATE TABLE`, no import/load step. Confirmed via `EXPLAIN` that selecting 2 of 19 columns produces a `PARQUET_SCAN` node whose `Projections` list only those 2 — the column-chunk layout from Lesson 4 being exploited by a real engine, not just hand-simulated.
- Second real cost of Lesson 4's missing statistics, beyond disabled row-group pruning: DuckDB's own cardinality estimate for `fare_amount > 100` was `~592,924` (a generic ~20%-selectivity fallback) against an actual count of `7,995` — off by ~74x. No stats means the query *optimizer* is also flying blind, which matters more once multi-table joins are involved (Phase 5).
- `read_parquet(glob, hive_partitioning=true)` exposes directory names (`year=2024/month=01`) as real queryable columns, even though they don't exist in the file's own schema at all — confirmed the dependency on this flag directly by reproducing the `BinderException` that occurs without it.
- DuckDB's role stays deliberately scoped: a convenience tool for Phase 3's SQL-shaped work, not a redefinition of what counts as data engineering. Phases 1-2's work (contracts, idempotency, validation, quarantine) is already data engineering regardless of which query engine does the aggregation.

## Lesson 8 — Cleaning and transforming trip records
- Curated is where we impose *our* naming conventions, not the source's — raw keeps `PULocationID`/`tpep_pickup_datetime` forever; curated renames to `pickup_location_id`/`pickup_datetime` because it's our derived product, not TLC's export.
- Reconciled two earlier decisions by scoping them: `clean_trips()` uses DuckDB internally for the SQL-shaped renaming/derivation, but keeps a strict Polars-in/Polars-out contract, so the pipeline isn't committed to DuckDB's data structures end to end.
- DuckDB can query a Polars DataFrame directly by Python variable name (`FROM valid_df`) — no file, no explicit registration step.
- Real, non-obvious finding: DuckDB's native microsecond timestamp resolution only affects *derived* columns (`pickup_hour`, computed via `date_trunc`) — a passthrough column (`pickup_datetime`) keeps its original nanosecond precision from the source file. Confirmed by checking both dtypes after the same query, not assumed.
- Curated column selection is a deliberate narrowing tied to the actual business problem (hourly pickup-demand forecasting) — dropped fare-breakdown detail and vendor/rate-code columns not needed for that question; raw still has them in full if a future need arises.
- Verified hour-bucketing correctness at both a within-day boundary (`10:59:59`→10, `11:00:00`→11) and a day boundary (`23:59:59` stays in hour 23) — boundary bugs in time-bucketing are a classic, easy-to-miss source of errors.

## Lesson 9 — Aggregating pickups by date/hour/zone
- A naive `GROUP BY` can only emit rows for combinations that occurred in the data — a zero-trip hour-zone pair produces no row at all, which looks identical to "missing data" rather than "zero demand." Proved this concretely: naive grouping gave 77,389 rows; the complete grid has 193,440 — 116,051 real zero-demand points (60%) would have silently vanished.
- Fix: build the complete grid first (every hour × every observed zone via a cross join), then left-join real counts and `fill_null(0)` — zero becomes explicit, not absent. This matters specifically because Phase 6's lag features (`demand 1 hour ago`) break the moment there's an unexplained gap in the time series.
- Tool choice is per-task, not all-or-nothing: Lesson 8 used DuckDB because renaming/`date_trunc` was SQL-shaped; this lesson's cross join + group-by + left join is equally natural in Polars' own API, and staying in Polars avoided re-triggering DuckDB's microsecond-precision downcast (Lesson 8's finding) on a column this join depends on.
- Verified rather than assumed: grid dimensions (744 hours × 260 observed zones = 193,440), count conservation (`pickup_count` sums to exactly the valid trip count, 2,895,468), and that the join key's dtype actually matched on both sides before trusting the result — a silent dtype mismatch here would have produced all-null counts, not an error.
- "Every zone" is honestly scoped: zones observed with *some* activity this month, not the official ~265-zone TLC list (not loaded yet). A zone with zero pickups all month is still absent — a documented limitation, not an oversight.
- `month_bounds()` was extracted from `quality.py` into `dates.py` only once a second real caller (this lesson) needed the identical calculation — refactoring on the second real use, not preemptively.

## Lesson 10 — Writing curated Parquet and understanding partitioning
- Partitioning granularity is a deliberate trade-off, not a default: too coarse loses pruning benefit as data accumulates; too fine creates the "small file problem." The right granularity matches the unit the pipeline actually processes in — ours is monthly end to end (ingest, validate, transform, aggregate), so curated partitions by `year=/month=` too, not by zone, even though zone-level queries matter for Phase 6.
- "Idempotent" doesn't always mean "skip if already done." `download_month()` skips re-downloading because downloading is expensive and external (Lesson 3). `write_curated()` always overwrites, with no skip check at all, because the aggregation is a deterministic, cheap-to-recompute function of raw + code — re-running should replace stale output, not preserve it. Both are genuinely idempotent; the mechanism differs because the economics differ.
- Confirmed directly, not assumed: writing a 1-row result over a prior 193,440-row file produces exactly 1 row back (replace, not merge), with no leftover `.tmp` file — the atomic write pattern from Lesson 3 carries over even though the skip-check doesn't.
- 47.6MB raw → 114KB curated (~400x reduction) — this is the actual size of the signal Phase 6 trains on; a curated layer's whole purpose is compressing business-relevant signal out of much larger raw data.
- Full-circle check: re-queried the curated layer with the exact same DuckDB Hive-partition glob from Lesson 7 and confirmed `total_pickups` sums to `2,895,468` end-to-end, matching the valid trip count from Lesson 6 — every layer agrees.

**Phase 3 (Transformation and analytical storage) is complete.**

## Open questions
- (none yet — add here as they come up)
