# Data Model

## Purpose

This document summarizes the intended table model based on `src/common/config.py` and the currently implemented Python pipeline.

## Catalog Layout

- Catalog: `fin_signals_dev`
- Schemas:
  - `bronze`
  - `silver`
  - `gold`
  - `audit`

## Bronze Tables

### `fin_signals_dev.bronze.market_prices_raw`

Current shape from ingestion code:

- `symbol`
- `price`
- `currency`
- `market_time`
- `ingested_at`
- `_ingest_date`
- `_source`

Notes:

- loaded from the Stooq CSV endpoint
- append-oriented landing table

### `fin_signals_dev.bronze.fx_rates_raw`

Current shape from ingestion code:

- `base_currency`
- `quote_currency`
- `rate`
- `rate_timestamp`
- `source_name`
- `source_url`
- `ingested_at`
- `run_id`

Notes:

- loaded from a public FX API endpoint
- append-oriented landing table

### `fin_signals_dev.bronze.macro_indicators_raw`

Current shape from ingestion code:

- `country_code`
- `indicator_name`
- `observation_date`
- `observation_value`
- `raw_payload`
- `ingest_ts`
- `_ingest_date`
- `_source`

Notes:

- loaded from the World Bank Indicators API
- raw payload is retained for traceability

## Silver Tables

### `fin_signals_dev.silver.market_prices`

Current behavior:

- filters null prices
- deduplicates by `symbol` and `market_time`
- casts `price` to `decimal(12,4)`
- overwrites the target table

Primary columns in current write path:

- `symbol`
- `price`
- `currency`
- `market_time`
- `ingested_at`
- `_ingest_date`
- `_source`

### `fin_signals_dev.silver.fx_rates`

Current behavior:

- filters null rates
- deduplicates by `base_currency`, `quote_currency`, and `rate_timestamp`
- derives `currency_pair`
- derives `rate_date`
- stores `source_system`
- merges into the target table when it already exists

Primary columns in current write path:

- `currency_pair`
- `base_currency`
- `quote_currency`
- `rate`
- `rate_timestamp`
- `rate_date`
- `source_system`
- `ingested_at`

### `fin_signals_dev.silver.macro_indicators`

Current behavior:

- casts `observation_date` to date
- casts `observation_value` to double
- filters incomplete records
- keeps the latest record per `country_code`, `indicator_name`, `observation_date`
- overwrites the target table

Primary columns in current write path:

- `country_code`
- `indicator_name`
- `observation_date`
- `observation_value`
- `raw_payload`
- `ingest_ts`
- `_ingest_date`
- `_source`

## Gold Tables

### `fin_signals_dev.gold.daily_market_snapshot`

Currently active.

Derived fields include:

- `snapshot_date`
- `latest_price`
- `open_price`
- `day_change`
- `day_change_pct`
- `currency`
- `market_time`
- `ingested_at`

### `fin_signals_dev.gold.fx_trend_signals`

Currently active.

Derived fields include:

- `currency_pair`
- `rate_date`
- `base_currency`
- `quote_currency`
- `rate`
- `daily_change`
- `daily_change_pct`
- `weekly_change_pct`
- `trend_signal`
- `ingested_at`

### `fin_signals_dev.gold.macro_indicator_trends`

Implemented in code but not currently active in the gold job runner.

Derived fields include:

- `country_code`
- `indicator_name`
- `observation_date`
- `observation_value`
- `period_change`
- `period_change_pct`
- `year_over_year_pct`
- `trend_direction`
- `ingested_at`

### `fin_signals_dev.gold.cross_signal_summary`

Implemented in code but not currently active in the gold job runner.

Derived fields aggregate the latest market, FX, and macro trend outputs by date.

## Audit Tables

### `fin_signals_dev.audit.pipeline_runs`

Current logged fields:

- `pipeline_name`
- `status`
- `row_count`
- `message`
- `run_timestamp`

### `fin_signals_dev.audit.ingest_events`

Configured in `src/common/config.py`, but verify active creation and usage before assuming it is fully wired.

## Notes

- Some SQL DDL files currently lag the implemented Python transformations.
- When contracts change, update config, code, orchestration, and documentation together.
