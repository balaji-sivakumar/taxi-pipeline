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

## Open questions
- (none yet — add here as they come up)
