# Financial Signals Lakehouse

Databricks Asset Bundles project for a market + FX + macro risk intelligence platform. The repo ingests public financial and macroeconomic datasets into Bronze, standardizes them in Silver, and publishes Gold risk tables that are designed to support treasury, risk, and advisory-style conversations in a live employer demo.

## What Is In Scope

- Bronze ingestion for market prices, FX rates, and macro indicators
- Silver transformation jobs for each domain
- Gold analytics jobs for market, FX, macro, and cross-signal risk tables
- Gold explainability output for top movers with cross-domain context
- Audit logging to `fin_signals_dev.audit`
- Databricks bundle deployment and job orchestration

## Current State

The repo is beyond bootstrap stage. The daily job defined in `resources/jobs/daily_pipeline_job.yml` runs at 06:30 UTC Mon–Fri:

1. Price Bronze ingest
2. Price Silver transform
3. FX Bronze ingest
4. FX Silver transform
5. Macro Bronze ingest
6. Macro Silver transform
7. Gold analytics
8. Validation queries
9. Export to S3 (JSON for the React dashboard)

There are two important implementation notes:

- Market and FX Bronze now support both `snapshot` and `backfill` modes through runtime env vars.
- A separate historical backfill job is available for one-time history loading before daily refreshes take over.

## Architecture

The project follows a standard medallion layout:

- Bronze: append-oriented raw landing tables with ingestion metadata
- Silver: typed, deduplicated, cleaned datasets
- Gold: curated analytical outputs for downstream reporting and signal consumption
- Audit: isolated run logging under `fin_signals_dev.audit`

Core naming is centralized in `src/common/config.py` and should be treated as the source of truth for catalog, schema, and table names.

## Public Data Sources

- Market prices: yfinance (Yahoo Finance) — 23 symbols including SPY, QQQ, Mag-7, sectors, commodities, DXY. Set `PRICE_SOURCE=stooq` to use Stooq instead (note: Stooq hard-caps at 5 rows).
- FX rates: Frankfurter public FX API
- Macro indicators: World Bank Indicators API

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
3. Run the historical backfill job once if you need deep analytical history
4. Run the daily pipeline job
5. Review validation output and audit tables

The bundle entrypoint is `databricks.yml`. The main orchestrated workflow is `resources/jobs/daily_pipeline_job.yml`.

## Reality Checks

- Some SQL, notebook, and documentation assets are still thinner than the Python pipeline code.
- Validation now performs freshness, duplicate, history-depth, and Gold output checks, with macro freshness and history depth evaluated using lower-frequency thresholds suitable for annual public indicators.
- If you change Silver or Gold behavior, update orchestration, docs, and any relevant DDL in the same change.

## Companion Projects

- **[financial-signals-dashboard](https://github.com/gveitch1972/financial-signals-dashboard)** — React/Vite SPA deployed at [quicksight.grahamveitch.com](https://quicksight.grahamveitch.com). Consumes the S3 JSON exports from step 9 above.
- **Power BI** — `powerbi/` folder contains a .pbix report connecting directly to the Gold tables via the Databricks native connector. Setup in `powerbi/setup.md`.
- **n8n Telegram bot** — `n8n Databricks controller 3.0.json` — trigger pipeline runs and check job status via Telegram commands (`/run`, `/status`, `/list`, `/help`). Uses Header Auth against the Databricks Jobs API.

## Documentation

- [Architecture](docs/architecture.md)
- [Data Model](docs/data_model.md)
- [Runbook](docs/runbook.md)
- [Security](docs/security.md)
- [Project Overview](PROJECT_OVERVIEW.md)
- [Agent Guidance](AGENTS.md)
