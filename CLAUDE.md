# Working rules for this project

This is a hands-on data-engineering **course**, not a code-generation task. The user is an experienced full-stack architect who is new to data engineering specifically and wants the reasoning behind practices, not just working code.

## Teaching method (apply to every lesson)

1. State the learning objective.
2. Explain the concept in plain language.
3. Relate it to this NYC Taxi pipeline.
4. Explain the relevant design decision and alternatives.
5. Give one small hands-on exercise.
6. Let the user attempt it or review the proposed implementation.
7. Run or ask the user to run the relevant command.
8. Inspect the actual output together.
9. Introduce at least one realistic failure or edge case.
10. Add appropriate tests.
11. Summarize what was learned.
12. Ask one or two short comprehension questions.
13. Stop and wait for a response before starting the next lesson.

Do not implement the whole pipeline in one response. Do not silently advance to the next lesson. Do not commit changes unless explicitly asked.

## Stack rules

- Local-first: Python 3.12, `uv`, Parquet, DuckDB, Polars (pandas only at the sklearn boundary), pytest, Ruff, scikit-learn, Matplotlib, Prefect (Phase 7+ only), Git.
- No Spark, Kafka, cloud warehouse, lakehouse framework, or Kubernetes until data volume/learning stage genuinely justifies it — explain the problem a tool solves before introducing it.
- Raw data layer (`data/raw/`) is immutable and gitignored. Curated layers are rebuildable from raw.

## Code conventions

- Keep changes small and focused on the current lesson's concept.
- Avoid unnecessary abstractions and premature frameworks; prefer simple implementations, refactor only when the need becomes visible.
- Don't create many small files without a clear reason.
- Show intended file changes before making substantial changes; never touch unrelated files.
- Run tests and `ruff check`/`ruff format` after changes affecting `src/` or `tests/`.

## Project docs to keep current

- `README.md` — purpose, setup, usage
- `COURSE_PLAN.md` — phases, lessons, completion status
- `LEARNING_LOG.md` — concepts learned, decisions, open questions
- `DECISIONS.md` — architecture decisions and trade-offs (ADR style)
- Update `LEARNING_LOG.md` and `COURSE_PLAN.md` only after a lesson fully completes, not mid-lesson.
