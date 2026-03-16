# AGENTS.md

## Purpose

Working agreement for agents editing this repository safely and with minimal drift from the deployed pipeline.

## Read First

1. `README.md`
2. `PROJECT_OVERVIEW.md`
3. `docs/architecture.md`
4. `databricks.yml`
5. `resources/jobs/daily_pipeline_job.yml`
6. `src/common/config.py`

## Source Of Truth Rules

When files disagree, trust them in this order:

1. `src/common/config.py` for catalog, schema, and table names
2. `databricks.yml` and `resources/jobs/*.yml` for orchestration
3. `src/**/*.py` for implemented behavior
4. `sql/*.sql` and `notebooks/*.py`
5. Markdown docs

Do not assume a file is current just because it exists. This repo contains some placeholder and partially aligned assets.

## Layer Boundaries

- `src/bronze/`: raw ingestion and landing
- `src/silver/`: standardization, typing, deduplication, validation
- `src/gold/`: curated outputs and analytical signals
- `src/common/`: shared config, audit, logging, and helpers

Keep those boundaries intact unless the task explicitly changes the design.

## Guardrails

- Preserve Bronze append-oriented behavior unless the task explicitly redesigns ingestion.
- Keep audit writes under `fin_signals_dev.audit` via `src/common/audit.py`.
- Use constants from `src/common/config.py`; do not hardcode table names.
- Prefer explicit schemas and deterministic selected columns in Spark transformations.
- Do not commit secrets, tokens, or credentials.
- Avoid changing job names or task keys unless the request is specifically about orchestration.

## Current Repo Reality

- Bronze, Silver, and Gold code all exist and are wired into the daily job.
- The daily job currently includes market, FX, and macro Bronze/Silver stages.
- The active gold runner currently builds only market snapshot and FX trend outputs.
- Macro gold and cross-signal gold code exist, but they are not active in the current gold runner.
- Validation is currently lightweight and includes placeholder behavior.
- Some SQL DDL files lag the active Python pipeline.

## Where To Edit

- Catalog/schema/table constants: `src/common/config.py`
- Bootstrap and environment setup: `src/bootstrap/bootstrap_env.py`, `sql/*.sql`
- Bronze ingestion: `src/bronze/*.py`
- Silver transforms: `src/silver/*.py`
- Gold outputs: `src/gold/*.py`
- Job runners and orchestration: `resources/jobs/*.py`, `resources/jobs/*.yml`, `databricks.yml`
- Validation hooks: `notebooks/99_validation_queries.py`, `resources/jobs/run_99_validation_queries.py`
- Tests: `src/tests/`

## Change Expectations

- Keep changes focused and minimal.
- Update docs when behavior, contracts, or job sequence changes.
- If you change Silver or Gold logic, check whether SQL, notebooks, and job wiring also need updates.
- If you change table contracts, include impact notes in your summary.
- Do not infer production readiness from a single file; verify against the job wiring and current implementation.

## Safe Defaults

- If a task touches table names, start in `src/common/config.py`.
- If a task touches execution order, start in `resources/jobs/daily_pipeline_job.yml`.
- If a task touches delivered behavior, confirm whether the runner script actually invokes the code path.
- If the repo contains duplicate or oddly named files, prefer the file referenced by job wiring unless the user asks to clean them up.
