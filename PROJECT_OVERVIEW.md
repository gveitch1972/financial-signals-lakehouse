# Financial Signals Lakehouse — Project Overview

## What this is

Databricks Asset Bundles project (`databricks.yml`) for a medallion lakehouse in `fin_signals_dev` with schemas:
`bronze`, `silver`, `gold`, `audit`.

## Architecture

- **Bootstrap**
  - `src/bootstrap/bootstrap_env.py`
  - SQL bootstrap: `sql/001_create_catalog.sql`, `sql/002_create_schemas.sql`
- **Bronze ingestion**
  - `src/bronze/ingest_market_data.py`
  - Runner: `resources/jobs/run_bronze_ingestion.py`
  - Bronze DDL: `sql/010_create_bronze_tables.sql`
  - Pattern: explicit schema + append-only Delta writes
- **Silver + Gold processing**
  - Orchestrated in `resources/jobs/daily_pipeline_job.yml`
  - Executed via `notebooks/02_silver_transform_demo.py` and `notebooks/03_gold_analytics_demo.py`
- **Audit (separate schema by design)**
  - Writer: `src/common/audit.py`
  - Table DDL: `sql/003_create_audit_tables.sql`
  - Table: `fin_signals_dev.audit.pipeline_runs`

## Main files

- Bundle/orchestration: `databricks.yml`, `resources/jobs/daily_pipeline_job.yml`
- Bootstrap: `src/bootstrap/bootstrap_env.py`
- Bronze ingest: `src/bronze/ingest_market_data.py`
- Shared constants: `src/common/config.py`
- Audit: `src/common/audit.py`

## Run / deploy flow

1. `databricks bundle deploy -t dev`
2. Run `bootstrap_environment` job (catalog/schemas)
3. Run Bronze ingest (`resources/jobs/run_bronze_ingestion.py`)
4. Run daily medallion job (`resources/jobs/daily_pipeline_job.yml`)
5. Run validation (`notebooks/99_validation_queries.py`) and confirm audit entries

## Current status

- Bronze ingestion: ✅
- Bootstrap and schema creation: ✅
- Audit schema isolation: ✅
- Silver transformation phase: ✅
- Gold analytics phase: ✅

> Note: this repo snapshot has minimal placeholder content in some Silver/Gold-linked files; treat the job pipeline and current team status as source of truth for delivery status.
