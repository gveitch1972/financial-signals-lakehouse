# AGENTS.md

## Purpose

Practical guidance for agents editing this repository safely.

## Quick start

1. Read `README.md`, `docs/architecture.md`, `PROJECT_OVERVIEW.md`.
2. Check job wiring in `databricks.yml` and `resources/jobs/daily_pipeline_job.yml`.
3. Use constants from `src/common/config.py` (do not hardcode table names).

## Guardrails

- Keep medallion boundaries:
  - `src/bronze/` = ingestion/raw landing
  - `src/silver/` = standardize/validate
  - `src/gold/` = curated outputs
- Preserve Bronze append-only behavior unless explicitly changing design.
- Keep audit writes in `fin_signals_dev.audit` via `src/common/audit.py`.
- Prefer explicit Spark schemas and deterministic selected columns.
- Never commit secrets/credentials.

## Current project state

- Bronze, Silver, and Gold are considered delivered pipeline phases.
- Daily orchestration flow is defined in `resources/jobs/daily_pipeline_job.yml`.
- Audit remains intentionally isolated under `fin_signals_dev.audit`.

## Repo reality checks

- Some Silver/Gold-linked files in this snapshot are minimal/placeholder.
- Do not infer production status from any single file; confirm against job wiring, runbook, and current team direction.
- If updating Silver/Gold behavior, keep orchestration, notebooks, SQL, and docs aligned in the same change.

## Where to edit

- Table/catalog names: `src/common/config.py`
- Bootstrap/DDL: `src/bootstrap/bootstrap_env.py`, `sql/*.sql`
- Jobs and orchestration: `databricks.yml`, `resources/jobs/*.yml`
- Tests/validation: `src/tests/`, `notebooks/99_validation_queries.py`

## Delivery expectations

- Keep changes focused and minimal.
- Update docs when behavior or data contracts change.
- Include impact notes whenever altering layer contracts or job sequence.
