# Runbook

## Purpose
Operational notes for running, monitoring, and recovering the Financial Signals Lakehouse pipeline.

## Daily Run Sequence
1. ingest market data to Bronze
2. ingest FX data to Bronze
3. ingest macro data to Bronze
4. transform Bronze to Silver
5. build Gold tables
6. write audit status

## Failure Handling
If a Bronze ingest fails:
- check source availability
- validate secret retrieval
- inspect audit logs
- rerun the source-specific ingest job

If a Silver transform fails:
- inspect schema drift
- review null/type failures
- validate upstream Bronze row counts

If a Gold build fails:
- verify dependent Silver tables were refreshed
- run validation notebook
- inspect metric logic changes

## Validation Checks
- row count not zero unless expected
- mandatory business keys present
- dates parsable
- duplicates within expected tolerance
- latest load timestamp updated
