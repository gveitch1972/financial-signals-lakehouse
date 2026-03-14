# Financial Signals Lakehouse — Project Overview

## Project type
Databricks Asset Bundles project for a medallion-style financial data platform.

- Bundle config: `databricks.yml`
- Catalog target: `fin_signals_dev`
- Schemas: `bronze`, `silver`, `gold`, `audit`

## Architecture (current implementation)
- **Bootstrap**
  - `src/bootstrap/bootstrap_env.py` creates catalog + schemas.
  - SQL equivalents exist in `sql/001_create_catalog.sql` and `sql/002_create_schemas.sql`.
- **Bronze**
  - Implemented ingestion: `src/bronze/ingest_market_data.py`.
  - Uses explicit Spark schema and append-only Delta write to `fin_signals_dev.bronze.market_prices_raw`.
  - Runner entrypoint: `resources/jobs/run_bronze_ingestion.py` and `notebooks/01_bronze_ingestion_demo.py`.
- **Audit**
  - `src/common/audit.py` appends run records to `fin_signals_dev.audit.pipeline_runs`.
  - Audit table DDL: `sql/003_create_audit_tables.sql`.
- **Silver / Gold**
  - Job orchestration references Silver/Gold notebook tasks.
  - Current notebook/script implementations are placeholders (`notebooks/02_*`, `notebooks/03_*`, `notebooks/99_*`; several `src/silver/*` and `src/gold/*` modules are empty).

## Current status
- Bronze ingestion foundation is in place (market feed path).
- Bootstrap works and creates required schemas.
- Bronze writes are append-only with explicit schema.
- Audit is intentionally isolated in its own schema.
- **Next phase:** implement real Silver transforms and table DDL, then wire Gold logic to non-placeholder code.

## Main files to start with
- `databricks.yml`
- `resources/jobs/daily_pipeline_job.yml`
- `resources/jobs/run_bronze_ingestion.py`
- `src/bootstrap/bootstrap_env.py`
- `src/bronze/ingest_market_data.py`
- `src/common/config.py`
- `src/common/audit.py`
- `sql/001_create_catalog.sql`, `sql/002_create_schemas.sql`, `sql/003_create_audit_tables.sql`, `sql/010_create_bronze_tables.sql`

## Practical run/deploy flow
1. Deploy bundle resources: `databricks bundle deploy -t dev`
2. Run bootstrap (`bootstrap_environment`) to ensure catalog/schemas exist.
3. Run Bronze ingestion (`resources/jobs/run_bronze_ingestion.py`).
4. Run daily pipeline job (`resources/jobs/daily_pipeline_job.yml`).
5. Validate with `notebooks/99_validation_queries.py` and `fin_signals_dev.audit.pipeline_runs`.

