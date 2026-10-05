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

## Open questions
- (none yet — add here as they come up)
