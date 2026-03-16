# Financial Signals Lakehouse

Databricks Asset Bundles project for a medallion-style financial data pipeline. The repo ingests public market, FX, and macroeconomic datasets into Bronze, standardizes them in Silver, and publishes analytics-ready outputs in Gold, with audit logging kept in a separate schema.

## What Is In Scope

- Bronze ingestion for market prices, FX rates, and macro indicators
- Silver transformation jobs for each domain
- Gold analytics jobs for market snapshot and FX trend signals
- Audit logging to `fin_signals_dev.audit`
- Databricks bundle deployment and job orchestration

## Current State

The repo is beyond bootstrap stage. The daily job defined in `resources/jobs/daily_pipeline_job.yml` currently runs:

1. Market Bronze ingest
2. Market Silver transform
3. FX Bronze ingest
4. FX Silver transform
5. Macro Bronze ingest
6. Macro Silver transform
7. Gold analytics
8. Validation step

There are two important implementation notes:

- The gold runner currently executes `daily_market_snapshot` and `fx_trend_signals`.
- Macro trend and cross-signal gold builders exist in `src/gold/`, but they are not active in the current gold job runner.

## Architecture

The project follows a standard medallion layout:

- Bronze: append-oriented raw landing tables with ingestion metadata
- Silver: typed, deduplicated, cleaned datasets
- Gold: curated analytical outputs for downstream reporting and signal consumption
- Audit: isolated run logging under `fin_signals_dev.audit`

Core naming is centralized in `src/common/config.py` and should be treated as the source of truth for catalog, schema, and table names.

## Public Data Sources

- Market prices: Stooq CSV endpoint
- FX rates: Frankfurter-style public FX API endpoint
- Macro indicators: World Bank Data360 API

These integrations are lightweight and intended for demonstration and portfolio-style engineering work rather than production vendor management.

## Repo Layout

- `src/bootstrap/`: environment bootstrap and schema setup
- `src/bronze/`: ingestion logic
- `src/silver/`: standardization and cleaning logic
- `src/gold/`: analytical output builders
- `src/common/`: shared config, audit, and utility code
- `resources/jobs/`: Databricks job runners and job YAML
- `sql/`: bootstrap and DDL assets
- `notebooks/`: demo and validation notebooks
- `docs/`: architecture, data model, runbook, and security notes

## Deploy And Run

1. Deploy the bundle: `databricks bundle deploy -t dev`
2. Run the bootstrap job to create the catalog, schemas, and audit objects
3. Run the daily pipeline job
4. Review validation output and audit tables

The bundle entrypoint is `databricks.yml`. The main orchestrated workflow is `resources/jobs/daily_pipeline_job.yml`.

## Reality Checks

- Some SQL, notebook, and documentation assets are still thinner than the Python pipeline code.
- Validation is currently a placeholder runner, not a full quality suite.
- If you change Silver or Gold behavior, update orchestration, docs, and any relevant DDL in the same change.

## Documentation

- [Architecture](docs/architecture.md)
- [Data Model](docs/data_model.md)
- [Runbook](docs/runbook.md)
- [Security](docs/security.md)
- [Project Overview](PROJECT_OVERVIEW.md)
- [Agent Guidance](AGENTS.md)
