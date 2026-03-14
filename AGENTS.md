# AGENTS.md

## Purpose
Operating guide for coding agents changing this repository.

## Quick orientation
1. Read: `README.md`, `docs/architecture.md`, `PROJECT_OVERVIEW.md`.
2. Check orchestration: `databricks.yml`, `resources/jobs/daily_pipeline_job.yml`.
3. Use shared table constants in `src/common/config.py` (avoid hardcoded table names).

## Guardrails
- Respect medallion boundaries:
  - `src/bronze/`: ingestion/raw landing
  - `src/silver/`: standardization/validation
  - `src/gold/`: curated analytics
- Keep Bronze ingestion append-only unless a task explicitly requires a change.
- Keep audit writes in `fin_signals_dev.audit` via `src/common/audit.py`.
- Prefer explicit Spark schemas and stable/selective column ordering.
- Never commit credentials or secrets.

## Current-reality notes
- Implemented ingestion logic currently lives in `src/bronze/ingest_market_data.py`.
- Multiple Silver/Gold scripts and SQL files are placeholders/empty; verify file content before assuming logic exists.
- Job YAML may reference notebook placeholders; keep orchestration and implementation aligned.

## Where to change things
- Table constants and names: `src/common/config.py`
- Bootstrap/env setup: `src/bootstrap/bootstrap_env.py` and `sql/*.sql`
- Job/task orchestration: `databricks.yml`, `resources/jobs/*.yml`
- Validation/tests: `notebooks/99_validation_queries.py`, `src/tests/`

## Delivery expectations
- Keep changes focused and minimal.
- Update docs when table contracts or run flow change.
- For Silver work, implement `src/silver/` first, then connect job/notebook entrypoints.
