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
