# Project Overview

## Summary

`financial-signals-lakehouse` is a Databricks Asset Bundles repo for a small financial data platform built around Bronze, Silver, Gold, and Audit schemas in the `fin_signals_dev` catalog.

The project is designed to show practical lakehouse engineering patterns:

- public API ingestion
- medallion layer separation
- centralized table naming
- Databricks job orchestration
- audit logging and validation hooks

Current public-source coverage uses:

- Stooq for market prices
- a public FX API for FX rates
- the World Bank Indicators API for macro data

## Source Of Truth

When repo files disagree, use this order of precedence:

1. `src/common/config.py` for table names
2. `databricks.yml` and `resources/jobs/daily_pipeline_job.yml` for deployed job behavior
3. Python pipeline code under `src/`
4. SQL and notebook assets
5. Markdown docs

This matters because parts of the repo are more complete than others, and some SQL or notebook artifacts lag behind the active Python workflow.

## Active Pipeline

### Bootstrap

- `databricks.yml`
- `src/bootstrap/bootstrap_env.py`
- `sql/001_create_catalog.sql`
- `sql/002_create_schemas.sql`
- `sql/003_create_audit_tables.sql`

### Bronze

- `src/bronze/ingest_market_data.py`
- `src/bronze/ingest_fx_data.py`
- `src/bronze/ingest_macro_data.py`

### Silver

- `src/silver/transform_market_data.py`
- `src/silver/transform_fx_data.py`
- `src/silver/transform_macro_data.py`

### Gold

- Active in runner:
  - `src/gold/build_daily_market_snapshot.py`
  - `src/gold/build_fx_trend_signals.py`
- Present in repo but not active in current runner:
  - `src/gold/build_macro_indicator_trends.py`
  - `src/gold/build_cross_signal_summary.py`

### Audit

- `src/common/audit.py`
- Target schema: `fin_signals_dev.audit`

## Daily Job Wiring

The main scheduled workflow is `resources/jobs/daily_pipeline_job.yml`.

Current tasks:

1. `bronze_ingestion`
2. `silver_transform`
3. `bronze_fx_ingestion`
4. `silver_fx_transform`
5. `bronze_macro_ingestion`
6. `silver_macro_transform`
7. `gold_analytics`
8. `validation_queries`

Current dependency shape:

- Market Silver depends on market Bronze
- FX Silver depends on FX Bronze
- Macro Silver depends on macro Bronze
- Gold depends on market Silver and FX Silver
- Validation depends on Gold

Macro Silver is wired into the job, but current Gold execution does not depend on it because the active gold runner only builds market and FX outputs.

## Data Products

Configured tables in `src/common/config.py`:

- Bronze:
  - `fin_signals_dev.bronze.market_prices_raw`
  - `fin_signals_dev.bronze.fx_rates_raw`
  - `fin_signals_dev.bronze.macro_indicators_raw`
- Silver:
  - `fin_signals_dev.silver.market_prices`
  - `fin_signals_dev.silver.fx_rates`
  - `fin_signals_dev.silver.macro_indicators`
- Gold:
  - `fin_signals_dev.gold.daily_market_snapshot`
  - `fin_signals_dev.gold.fx_trend_signals`
  - `fin_signals_dev.gold.macro_indicator_trends`
  - `fin_signals_dev.gold.cross_signal_summary`
- Audit:
  - `fin_signals_dev.audit.pipeline_runs`
  - `fin_signals_dev.audit.ingest_events`

Not every configured table is fully represented in current SQL DDL or active job steps, so treat the config plus job wiring as the operational baseline.

## Known Gaps

- Validation runner is still a placeholder.
- Gold macro and cross-signal builders are not active in `resources/jobs/run_gold_analytics.py`.
- Some SQL DDL files do not yet mirror the full set of configured Silver and Gold tables.
- A duplicate-looking file exists at `src/gold/build_macro_indicator_trends 2nd.py` and should not be treated as the primary implementation.

## Working Norms

- Keep Bronze append-oriented unless deliberately redesigning ingestion.
- Use centralized config constants instead of hardcoded table names.
- Keep docs, orchestration, and code aligned when changing layer contracts.
- Preserve audit logging behavior when modifying pipeline stages.
