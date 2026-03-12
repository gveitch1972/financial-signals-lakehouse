# Data Model

## Catalog Structure

### Catalog
- fin_signals_dev

### Schemas
- bronze
- silver
- gold
- audit

## Bronze Tables

### bronze.market_prices_raw
Purpose:
- raw market price records as received from source

Suggested columns:
- source_name
- ingest_ts
- batch_id
- symbol
- trade_date
- open
- high
- low
- close
- volume
- raw_payload

### bronze.fx_rates_raw
Suggested columns:
- source_name
- ingest_ts
- batch_id
- base_currency
- quote_currency
- rate_date
- exchange_rate
- raw_payload

### bronze.macro_indicators_raw
Suggested columns:
- source_name
- ingest_ts
- batch_id
- country_code
- indicator_name
- observation_date
- observation_value
- raw_payload

## Silver Tables

### silver.market_prices
Purpose:
- typed and standardised market prices

Suggested columns:
- symbol
- trade_date
- open_price
- high_price
- low_price
- close_price
- volume
- source_name
- ingest_ts

### silver.fx_rates
Suggested columns:
- currency_pair
- rate_date
- exchange_rate
- base_currency
- quote_currency
- source_name
- ingest_ts

### silver.macro_indicators
Suggested columns:
- country_code
- indicator_name
- observation_date
- observation_value
- source_name
- ingest_ts

## Gold Tables

### gold.daily_market_snapshot
Purpose:
- current and recent market movement summary

### gold.fx_trend_signals
Purpose:
- rolling FX change indicators

### gold.macro_indicator_trends
Purpose:
- period-over-period movement in macro indicators

### gold.cross_signal_summary
Purpose:
- combined view across market, FX, and macro datasets
