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

1. `bronze_ingestion`
2. `silver_transform`
3. `bronze_fx_ingestion`
4. `silver_fx_transform`
5. `bronze_macro_ingestion`
6. `silver_macro_transform`
7. `gold_analytics`
8. `validation_queries`

## Normal Run Sequence

1. Deploy the bundle to the target workspace.
2. Run the bootstrap job if the environment is new or schemas need recreating.
3. Run the daily pipeline job.
4. Review task logs in Databricks.
5. Check audit writes in `fin_signals_dev.audit.pipeline_runs`.
6. Review validation output.

## What "Healthy" Looks Like

- Bronze jobs write fresh rows to their raw tables.
- Silver jobs complete without schema or type failures.
- Gold job writes `daily_market_snapshot` and `fx_trend_signals`.
- Audit entries show successful pipeline runs with non-null timestamps.
- Validation step completes, even though it is currently lightweight.

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
- Delta merge behavior for FX Silver

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

- whether the issue is a real pipeline error or placeholder validation behavior
- whether Gold outputs were refreshed before validation ran

## Recovery Notes

- Bronze tables are intended to remain append-oriented.
- Silver market and Silver macro currently overwrite their targets.
- Silver FX uses merge behavior when the target exists.
- Gold outputs currently overwrite their targets.

Be careful when changing write modes because they affect replay, idempotency, and downstream expectations.

## Operational Caveats

- Macro Silver runs in the daily job, but macro Gold is not currently active in the runner.
- Cross-signal summary code exists, but it is not currently part of the active Gold execution path.
- Validation is not yet a full data quality suite.
