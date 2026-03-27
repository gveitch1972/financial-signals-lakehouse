# Runbook

## Purpose

Operational notes for running, checking, and recovering the Financial Signals Lakehouse pipeline.

## Primary Jobs

### Bootstrap

- Bundle file: `databricks.yml`
- Job key: `bootstrap_environment`
- Purpose: create catalog, schemas, and audit structures

### Daily Pipeline

- File: `resources/jobs/daily_pipeline_job.yml`
- Job key: `financial_signals_daily_pipeline`

Current task order:

1. `bronze_price_ingestion`
2. `silver_price_transform`
3. `bronze_fx_ingestion`
4. `silver_fx_transform`
5. `bronze_macro_ingestion`
6. `silver_macro_transform`
7. `gold_analytics`
8. `validation_queries`

### Historical Backfill

- File: `resources/jobs/historical_backfill_job.yml`
- Job key: `financial_signals_historical_backfill`
- Purpose: one-time price and FX history loading plus downstream rebuild

## Normal Run Sequence

1. Deploy the bundle to the target workspace.
2. Run the bootstrap job if the environment is new or schemas need recreating.
3. Run the historical backfill job once to seed market and FX history.
4. Run the daily pipeline job for ongoing refreshes.
5. Review task logs in Databricks.
6. Check audit writes in `fin_signals_dev.audit.pipeline_runs`.
7. Review validation output.

## Demo Run Order

1. Confirm bootstrap has already been run for the workspace.
2. Confirm the historical backfill job has completed successfully.
3. Run the daily pipeline job to refresh the latest Gold outputs.
4. Open `notebooks/04_employer_demo_walkthrough.py`.
5. Show the latest risk regime, stressed instruments, FX signals, macro trends, and closing consultancy narrative.

## What "Healthy" Looks Like

- Bronze jobs write fresh rows to their raw tables.
- Silver jobs complete without schema or type failures.
- Gold job writes `daily_market_snapshot`, `fx_trend_signals`, `macro_indicator_trends`, and `cross_signal_summary`.
- Gold job also writes `top_movers_why` for latest-date explainability context.
- Audit entries show successful pipeline runs with non-null timestamps.
- Validation step completes with real data quality checks.

For demo storytelling, open `notebooks/06_risk_command_center.py` after the daily job finishes.

## Failure Handling

### Bronze ingest failure

Check:

- public endpoint availability
- HTTP failures or timeouts
- schema mismatch between source payload and ingestion code
- target table existence if bootstrap was skipped
- audit entries for the failing pipeline

Typical response:

1. confirm the source endpoint is reachable
2. rerun the affected Bronze task
3. if needed, rerun the matching Silver task after Bronze succeeds

### Silver transform failure

Check:

- upstream Bronze row availability
- unexpected nulls in required fields
- type cast failures
- duplicate key handling in deterministic Silver rebuilds

Typical response:

1. inspect the upstream Bronze table
2. confirm the transform still matches the source schema
3. rerun the affected Silver task

### Gold failure

Check:

- successful completion of market and FX Silver tasks
- schema drift in Silver outputs
- derived column assumptions in the Gold builders

Typical response:

1. rerun the failed Gold task after Silver is healthy
2. review whether the runner script is invoking the intended builders

### Validation failure

Check:

- whether the issue is a real pipeline error or a failed validation rule
- whether Gold outputs were refreshed before validation ran

## Recovery Notes

- Bronze tables are intended to remain append-oriented.
- Silver market and Silver macro currently overwrite their targets.
- Silver FX currently overwrites from deduplicated Bronze input.
- Gold outputs currently overwrite their targets.

Be careful when changing write modes because they affect replay, idempotency, and downstream expectations.

## Operational Caveats

- Price and FX Bronze support `snapshot` and `backfill` modes through runtime environment controls.
- Validation enforces minimum analytical history depth for the active Gold metrics, with lower macro thresholds to reflect annual-style public indicators.
